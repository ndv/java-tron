package org.tron.capture;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.db.Manager;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.core.net.messagehandler.TransactionsMsgHandler;
import org.tron.leveldb.CompressionType;
import org.tron.leveldb.DB;
import org.tron.leveldb.Options;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.contract.AssetIssueContractOuterClass.TransferAssetContract;
import org.tron.protos.contract.BalanceContract;
import org.tron.protos.contract.BalanceContract.AccountIdentifier;
import org.tron.protos.contract.BalanceContract.BlockBalanceTrace.BlockIdentifier;
import org.tron.protos.contract.BalanceContract.TransferContract;

import static org.tron.common.utils.StringUtil.encode58Check;

@Slf4j(topic = "net")
@Component
public class TransactionCapture {
  @Autowired
  private Wallet wallet;
  @Autowired
  private Manager manager;
  @Autowired
  private TransactionsMsgHandler transactionsMsgHandler;

  private Timer timer = new Timer("capture-timer");

  private DB db;

  private BlockingQueue<Transaction> transactionQueue = new ArrayBlockingQueue<Transaction>(10000);
  private long queueFullTransactionLogged;

  private File scriptDir;
  private long scriptDirLastModified; // track changed in the script directory
  private String commandLine;
  private Process captureProcess;
  private PrintStream processStdin;
  private Thread[] readers;
  private Thread scriptThread;

  Cache<Long, Boolean> capturedTransactions = CacheBuilder.newBuilder()
          .maximumSize(10000)
          .build();

  @PostConstruct
  public void init() {
    manager.transactionCapture = this;
    transactionsMsgHandler.transactionCapture = this;
    start();
  }

  @PreDestroy
  public synchronized void close() {
    try {
      timer.cancel();
      // help GC
      timer = null;
    } catch (Exception e) {
      logger.warn("capture-timer cancel error", e);
    }
    if (db != null) {
      try {
        db.close();
      } catch (IOException e) {
        logger.warn("Can't close the key database properly", e);
      }
    }
    shutdownProcess();
  }

  private synchronized void shutdownProcess() {
    if (captureProcess != null && captureProcess.isAlive()) {
      captureProcess.destroy();
      captureProcess = null;
      if (readers != null) {
        for (Thread th : readers) {
          th.interrupt();
        }
      }
      readers = null;
    }
    if (scriptThread != null) {
      scriptThread.interrupt();
      scriptThread = null;
    }
  }

  private byte[] getTargetAddress(byte[] address) {
    byte[] key = new byte[8];
    System.arraycopy(address, 1, key, 0, 8);
    return db.get(key);
  }

  private static long longHashCode(ByteString s) {
    long r = 0;
    for (int i = 0; i < s.size(); i++) {
      r = r >>> 8;
      r += s.byteAt(i);
    }
    return r;
  }

  public void capture(Transaction trx, boolean frompool) {
    if (db == null) {
      return;
    }

    Any any = trx.getRawData().getContract(0).getParameter();

    // check for duplicates
    long txhash = longHashCode(any.getValue());
    if (capturedTransactions.getIfPresent(txhash) != null) {
      return;
    }
    capturedTransactions.put(txhash, true);

    if (!transactionQueue.offer(trx)) {
      if (queueFullTransactionLogged + 1000 < System.currentTimeMillis()) {
        queueFullTransactionLogged = System.currentTimeMillis();
        logger.warn("The capture script is slow: skipping transactions...");
      }
      return;
    } else {
      if (queueFullTransactionLogged > 0) {
        queueFullTransactionLogged = 0;
        logger.warn("Capture script is back to normal");
      }
    }
  }

  class AccountData {
    long balance;
    long energy;

    public AccountData(long balance, long energy) {
      this.balance = balance;
      this.energy = energy;
    }
  }

  private AccountData getAccountData(ByteString addr) throws ItemNotFoundException {
    AccountIdentifier accId = AccountIdentifier.newBuilder()
            .setAddress(addr).build();
    BlockCapsule blockCapsule = wallet.getNowBlockCapsule();
    BlockCapsule.BlockId bid = blockCapsule.getBlockId();
    ByteString hashString = ByteString.copyFrom(bid.getBytes());
    BlockIdentifier blockId = BlockIdentifier.newBuilder()
            .setNumber(bid.getNum())
            .setHash(hashString)
            .build();
    return new AccountData(
            wallet.getAccountBalance(
                    BalanceContract.AccountBalanceRequest.newBuilder()
                            .setAccountIdentifier(accId)
                            .setBlockIdentifier(blockId)
                            .build()).getBalance(),
            wallet.getAccountResource(addr).getEnergyUsed()
    );
  }

  private void scriptThread() {
    while (!scriptThread.isInterrupted()) {
      try {
        Transaction trx = transactionQueue.take();

        int type = trx.getRawData().getContract(0).getType().getNumber();
        Any any = trx.getRawData().getContract(0).getParameter();

        byte[] priv;
        switch (type) {
          case ContractType.TransferContract_VALUE:
            TransferContract transferContract = any.unpack(TransferContract.class);
            priv = getTargetAddress(transferContract.getToAddress().toByteArray());
            if (priv != null) {
              AccountData ad = getAccountData(transferContract.getToAddress());
              processStdin.println("type=transfer");
              processStdin.println("from="
                      + encode58Check(transferContract.getOwnerAddress().toByteArray()));
              processStdin.println("to="
                      + encode58Check(transferContract.getToAddress().toByteArray()));
              processStdin.println("priv=" + Hex.toHexString(priv));
              processStdin.println("amount=" + transferContract.getAmount());
              processStdin.println("balance=" + ad.balance);
              processStdin.println("energy=" + ad.energy);
              processStdin.println();
              processStdin.flush();
            }
            break;
          case ContractType.TransferAssetContract_VALUE:
            TransferAssetContract assetContract = any.unpack(TransferAssetContract.class);
            priv = getTargetAddress(assetContract.getToAddress().toByteArray());
            if (priv != null) {
              AccountData ad = getAccountData(assetContract.getToAddress());
              processStdin.println("type=asset_transfer");
              processStdin.println("from="
                      + encode58Check(assetContract.getOwnerAddress().toByteArray()));
              processStdin.println("to="
                      + encode58Check(assetContract.getToAddress().toByteArray()));
              processStdin.println("priv=" + Hex.toHexString(priv));
              processStdin.println("amount=" + assetContract.getAmount());
              processStdin.println("asset="
                      + new String(assetContract.getAssetName().toByteArray()));
              processStdin.println("balance=" + ad.balance);
              processStdin.println("energy=" + ad.energy);
              processStdin.println();
              processStdin.flush();
            }
            break;
        }
      } catch (InvalidProtocolBufferException e) {
        logger.warn("In transaction capture", e);
      } catch (InterruptedException | ItemNotFoundException ex) {
        return;
      }
    }
  }

  private void checkScryptChanged() {
    if (scriptDirLastModified < scriptDir.lastModified()) {
      scriptDirLastModified = scriptDir.lastModified();
      logger.info("script directory changed; restarting the process");
      shutdownProcess();
      createProcess();
    }
  }

  public void start() {
    Properties props = new Properties();
    try (FileInputStream fis = new FileInputStream("capture.props")) {
      props.load(fis);
    } catch (FileNotFoundException e) {
      logger.warn("Can't find capture.props; won't capture transactions");
      return;
    } catch (IOException e) {
      logger.warn("Can't load capture.props" + e + "; won't capture transactions");
      return;
    }

    File dirFile = new File(props.getProperty("keydb", "keydb"));

    Options options = new Options().compressionType(CompressionType.NONE).createIfMissing(false);
    try {
      db = new DB(dirFile, options);
    } catch (IOException e) {
      logger.error("Can't open " + dirFile.getAbsolutePath() + ": " + e);
      return;
    }

    scriptDir = new File(props.getProperty("script_dir", "."));

    if (!scriptDir.isDirectory()) {
      logger.error("Directory " + scriptDir.getAbsolutePath() +
              " does not exist or not a directory; won't capture transactions");
      return;
    }

    scriptDirLastModified = scriptDir.lastModified();

    commandLine = props.getProperty("script");

    if (commandLine == null) {
      logger.error("No script property set in capture.props; won't capture transactions");
      return;
    }

    createProcess();

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        checkScryptChanged();
      }
    }, 10, 1000);

  }

  private static List<String> parseCommandLine(String cmd) {
    List<String> args = new ArrayList<>();
    boolean inQuotes = false;
    boolean inSpace = true;
    int argStart = 0;
    cmd += ' ';
    for (int i = 0; i < cmd.length(); i++) {
      char c = cmd.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          inQuotes = false;
          args.add(cmd.substring(argStart, i));
          inSpace = true;
        }
      } else if (inSpace) {
        if (c == '"') {
          inQuotes = true;
          argStart = i + 1;
          inSpace = false;
        } else if (c != ' ') {
          inSpace = false;
          argStart = i;
        }
      } else {
        if (c == ' ') {
          inSpace = true;
          args.add(cmd.substring(argStart, i));
        }
      }
    }
    return args;
  }

  class ReadLogThread extends Thread {
    private final InputStream is;
    private final String name;

    ReadLogThread(InputStream is, String name) {
      super(name + " reader");
      this.is = is;
      this.name = name;
    }

    @Override
    public void run() {
      byte[] buf = new byte[1024];
      while (!isInterrupted()) {
        try {
          int a = is.available();
          int offs = 0;
          if (a == 0) {
            int b = is.read();
            if (b == -1) {
              return;
            }
            buf[0] = (byte) b;
            offs = 1;
          }
          a = is.available();
          int nread = is.read(buf, offs, a > buf.length - offs ? buf.length - offs : a);
          logger.warn(name + ": "
                  + new String(buf, 0, nread + offs, Charset.defaultCharset()));
        } catch (IOException e) {
          Process p = captureProcess;
          if (p != null) {
            if (!p.isAlive()) {
              logger.warn("Capture process exited with code " + p.exitValue());
              shutdownProcess();
            }
          }
          return;
        }
      }
    }
  }

  private synchronized void createProcess() {
    if (scriptThread != null) {
      scriptThread.interrupt();
      scriptThread = null;
    }

    try {
      captureProcess = new ProcessBuilder()
              .command(parseCommandLine(commandLine))
              .directory(scriptDir)
              .start();
      processStdin = new PrintStream(
              new BufferedOutputStream(captureProcess.getOutputStream()),
              false,
              "US-ASCII");
      readers = new Thread[2];
      readers[0] = new ReadLogThread(captureProcess.getInputStream(), "stdout");
      readers[0].start();
      readers[1] = new ReadLogThread(captureProcess.getErrorStream(), "stderr");
      readers[1].start();

      scriptThread = new Thread(() -> {
        scriptThread();
      }, "script thread");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
