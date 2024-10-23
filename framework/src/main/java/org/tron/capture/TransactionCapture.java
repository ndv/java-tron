package org.tron.capture;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.http.nio.NHttpServerConnection;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.api.GrpcAPI;
import org.tron.common.crypto.Hash;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.ByteUtil;
import org.tron.common.utils.Commons;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.Wallet;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.db.Manager;
import org.tron.core.db.TransactionStore;
import org.tron.core.exception.BadItemException;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.core.net.messagehandler.TransactionsMsgHandler;
import org.tron.core.store.TransactionRetStore;
import org.tron.leveldb.CompressionType;
import org.tron.leveldb.DB;
import org.tron.leveldb.Options;
import org.tron.protos.Protocol.InternalTransaction;
import org.tron.protos.Protocol.InternalTransaction.CallValueInfo;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.contract.AssetIssueContractOuterClass.TransferAssetContract;
import org.tron.protos.contract.Common;
import org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract;
import org.tron.protos.contract.BalanceContract;
import org.tron.protos.contract.BalanceContract.AccountIdentifier;
import org.tron.protos.contract.BalanceContract.BlockBalanceTrace.BlockIdentifier;
import org.tron.protos.contract.BalanceContract.TransferContract;

import static org.tron.common.utils.StringUtil.encode58Check;
import static org.tron.common.utils.StringUtil.hexString2ByteString;

@Slf4j(topic = "capture")
@Component
public class TransactionCapture {
  @Autowired
  private Wallet wallet;
  @Autowired
  private Manager manager;
  @Autowired
  private TransactionsMsgHandler transactionsMsgHandler;
  @Autowired
  private TransactionStore transactionStore;

  @Autowired
  private TransactionRetStore transactionRetStore;

  private Timer timer = new Timer("capture-timer");

  private DB db;
  private byte[] bloom;
  private int bloomHashes;

  class PrintWriterWithSignal {
    PrintWriterWithSignal(PrintWriter w) {
      this.w = w;
      this.s = new CountDownLatch(1);
    }

    public PrintWriter w;
    public CountDownLatch s;
    public boolean force;
  }

  class TransactionAndPrintWriter {
    public TransactionAndPrintWriter(Transaction tx, PrintWriterWithSignal trace, boolean frompool) {
      this.tx = tx;
      this.trace = trace;
      this.frompool = frompool;
    }

    public Transaction tx;
    public PrintWriterWithSignal trace;
    public boolean frompool;
  }

  private BlockingQueue<TransactionAndPrintWriter> transactionQueue = new ArrayBlockingQueue<>(10000);
  private long queueFullTransactionLogged;

  private File scriptDir;
  private long scriptDirLastModified; // track changed in the script directory
  private String commandLine;
  private Process captureProcess;
  private PrintStream processStdin;
  private Thread[] readers;
  private Thread scriptThread;

  byte[] transferSelector = methodSelector("transfer(address,uint256)");
  byte[] transferFromSelector = methodSelector("transferFrom(address,address,uint256)");
  List<byte[]> trc20Contracts = new ArrayList<>();

  ThreadLocal<PrintWriterWithSignal> trace = new ThreadLocal<>();

  String[] patterns = new String[0];

  Cache<Long, Boolean> capturedTransactions = CacheBuilder.newBuilder()
          .maximumSize(10000)
          .build();

  private static byte[] methodSelector(String methodSignature) {
    byte[] signature = new byte[4];
    System.arraycopy(Hash.sha3(methodSignature.getBytes()), 0, signature, 0, 4);
    return signature;
  }

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
    stopTraceService();
  }

  private static int rotr(int a, int n) {
    return (a >>> n) | (a << (32 - n));
  }

  void tracePrintf(String msg, Object... args) {
    PrintWriterWithSignal t = trace.get();
    if (t != null) {
      t.w.printf(msg, args);
    }
  }

  void traceFinish() {
    PrintWriterWithSignal t = trace.get();
    if (t != null)
      t.s.countDown();
  }

  boolean traceForce() {
    PrintWriterWithSignal t = trace.get();
    if (t != null)
      return t.force;
    return false;
  }

  private byte[] getTargetAddress(byte[] address) {
    // check with the bloom filter
    ByteBuffer bb = ByteBuffer.wrap(address).order(ByteOrder.LITTLE_ENDIAN);
    long sz = bloom.length * 8L;
    for (int i = 0; i < bloomHashes; i++) {
      int a = bb.getInt(1 + i * 4);
      int b = bb.getInt(1 + (i + 2) % 5 * 4);
      long h = ((((long) (a ^ rotr(b, 17))) & 0xffffffffL) << 31)
              ^ (((long) (rotr(b, 29) ^ rotr(a, 7))) & 0xffffffffL);
      long bi = h % sz;
      if ((bloom[(int) (bi / 8)] & (1 << (bi % 8))) == 0) {
        tracePrintf("Address not found in Bloom filter: %s\r\n", ByteArray.toHexString(address));
        if (traceForce())
          return new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
        return null;
      }
    }

    tracePrintf("Address found in Bloom: %s\r\n", ByteArray.toHexString(address));

    byte[] key = new byte[8];
    System.arraycopy(address, 1, key, 0, 8);
    byte[] result = db.get(key);
    if (result == null && trace.get() != null) {
      tracePrintf("Bloom false positive: %s\r\n", ByteArray.toHexString(address));
      if (traceForce())
        return new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
    } else
      tracePrintf("Key found for: %s\r\n", ByteArray.toHexString(address));
    return result;
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
    long txhash = longHashCode(any.getValue()) ^ trx.getRawData().getTimestamp();
    if (capturedTransactions.getIfPresent(txhash) != null) {
      tracePrintf("Repeated transaction: (timestamp %d)\r\n", trx.getRawData().getTimestamp());
      logger.debug("Ignore repeated transaction (timestamp " + trx.getRawData().getTimestamp() + ") frompool=" + frompool);
      if (!traceForce()) {
        traceFinish();
        return;
      }
    }
    capturedTransactions.put(txhash, true);

    if (!transactionQueue.offer(new TransactionAndPrintWriter(trx, trace.get(), frompool))) {
      if (queueFullTransactionLogged + 1000 < System.currentTimeMillis()) {
        queueFullTransactionLogged = System.currentTimeMillis();
        logger.warn("The capture script is slow: skipping transactions...");
        tracePrintf("The capture script is slow: skipping transaction\r\n");
      } else {
        logger.error("No space in the queue, skipping the transaction...");
        tracePrintf("No space in queue\r\n");
      }
      traceFinish();
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
    long bandwidth;

    public AccountData(long balance, long energy, long bandwidth) {
      this.balance = balance;
      this.energy = energy;
      this.bandwidth = bandwidth;
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

    long e = 0;
    long b = 0;

    try {
      GrpcAPI.AccountResourceMessage ar = wallet.getAccountResource(addr);
      e = ar.getEnergyLimit() - ar.getEnergyUsed();
      b = ar.getFreeNetLimit() - ar.getFreeNetUsed()
              + ar.getNetLimit() - ar.getNetUsed();
    } catch (Exception ex) {
    }
    return new AccountData(
            wallet.getAccountBalance(
                    BalanceContract.AccountBalanceRequest.newBuilder()
                            .setAccountIdentifier(accId)
                            .setBlockIdentifier(blockId)
                            .build()).getBalance(),
            e,
            b
    );
  }

  private static String getTxId(Transaction trx) {
    return Sha256Hash.of(CommonParameter.getInstance().isECKeyCryptoEngine(),
            trx.getRawData().toByteArray()).toString();
  }

  private static byte[] getTxIdBytes(Transaction trx) {
    return Sha256Hash.of(CommonParameter.getInstance().isECKeyCryptoEngine(),
            trx.getRawData().toByteArray()).getBytes();
  }

  private void processPrintln(String s) {
    processStdin.println(s);
    if (trace.get() != null) trace.get().w.println(s);
  }

  void logTransactionCaptured(TransactionAndPrintWriter tp, byte[] address, String type)
  {
    Transaction.Result.contractResult ret = tp.tx.getRet(0).getContractRet();
    logger.info("Transaction captured: {} from {}, type: {} to {}, contractResult={}",
            getTxId(tp.tx), tp.frompool ? "POOL" : "BLOCK", type,
            Hex.toHexString(address), ret == null ? "null" : ret.name());
  }

  String getContractResult(Transaction trx)
  {
    Transaction.Result.contractResult ret = trx.getRet(0).getContractRet();
    return ret == null ? "null" : ret.name();
  }

  private void scriptThread() {
    while (!scriptThread.isInterrupted()) {
      try {
        TransactionAndPrintWriter tp = transactionQueue.take();
        Transaction trx = tp.tx;
        PrintWriterWithSignal trace = tp.trace;

        this.trace.set(trace);

        try {
          tracePrintf("Script thread got the transaction %s\r\n", trx.toString());

          int type = trx.getRawData().getContract(0).getType().getNumber();
          Any any = trx.getRawData().getContract(0).getParameter();

          tracePrintf("Transaction type %d\r\n", type);

          byte[] priv;
          byte[] address;
          switch (type) {
            case ContractType.TransferContract_VALUE:
              TransferContract transferContract = any.unpack(TransferContract.class);
              address = transferContract.getToAddress().toByteArray();
              priv = getTargetAddress(address);
              if (priv != null) {
                logTransactionCaptured(tp, address, "transfer");
                //AccountData ad = getAccountData(transferContract.getToAddress());
                processPrintln("type=transfer");
                //processPrintln("from="
                //        + Hex.toHexString(transferContract.getOwnerAddress().toByteArray()));
                processPrintln("to="
                        + Hex.toHexString(address));
                processPrintln("priv=" + Hex.toHexString(priv));
                processPrintln("amount=" + transferContract.getAmount());
                //processPrintln("balance=" + ad.balance);
                //processPrintln("energy=" + ad.energy);
                //processPrintln("bandwidth=" + ad.bandwidth);
                processPrintln("txid=" + getTxId(trx));
                processPrintln("contractResult=" + getContractResult(trx));
                processPrintln("");
                processStdin.flush();
              }
              break;
            case ContractType.TransferAssetContract_VALUE:
              TransferAssetContract assetContract = any.unpack(TransferAssetContract.class);
              address = assetContract.getToAddress().toByteArray();
              priv = getTargetAddress(address);
              if (priv != null) {
                logTransactionCaptured(tp, address, "asset_transfer");
                //AccountData ad = getAccountData(assetContract.getToAddress());
                processPrintln("type=asset_transfer");
                //processPrintln("from="
                //        + Hex.toHexString(assetContract.getOwnerAddress().toByteArray()));
                processPrintln("to=" + Hex.toHexString(address));
                processPrintln("priv=" + Hex.toHexString(priv));
                processPrintln("amount=" + assetContract.getAmount());
                processPrintln("asset="
                        + new String(assetContract.getAssetName().toByteArray()));
                //processPrintln("balance=" + ad.balance);
                //processPrintln("energy=" + ad.energy);
                //processPrintln("bandwidth=" + ad.bandwidth);
                processPrintln("txid=" + getTxId(trx));
                processPrintln("contractResult=" + getContractResult(trx));
                processPrintln("");
                processStdin.flush();
              }
              break;
            case ContractType.TriggerSmartContract_VALUE:
              TriggerSmartContract smartContract = any.unpack(TriggerSmartContract.class);

              if (!captureThisContract(smartContract.getContractAddress())) {
                logger.warn("This contract is not captured: " + Hex.toHexString(smartContract.getContractAddress().toByteArray()));
                break;
              }

              BigInteger amount;
              if (equals(smartContract.getData(), transferSelector, 4)) {
                address = unpackAddress(smartContract.getData(), 4);
                amount = unpackUint256(smartContract.getData(), 4 + 32);
              } else if (equals(smartContract.getData(), transferFromSelector, 4)) {
                address = unpackAddress(smartContract.getData(), 4 + 32);
                amount = unpackUint256(smartContract.getData(), 4 + 32 + 32);
              } else {
                logger.warn("Unknown method ID: " + smartContract.getData());
                tracePrintf("Unknown method ID");
                break;
              }
              priv = getTargetAddress(address);
              if (priv == null) {
                break;
              }
              logTransactionCaptured(tp, address, "trc20");

              //AccountData ad = getAccountData(ByteString.copyFrom(address));
              processPrintln("type=trc20");
              processPrintln("to=" + Hex.toHexString(address));
              processPrintln("priv=" + Hex.toHexString(priv));
              processPrintln("amount=" + amount);
              processPrintln("token="
                      + Hex.toHexString(smartContract.getContractAddress().toByteArray()));
              //processPrintln("balance=" + ad.balance);
              //processPrintln("energy=" + ad.energy);
              //processPrintln("bandwidth=" + ad.bandwidth);
              processPrintln("txid=" + getTxId(trx));
              processPrintln("contractResult=" + getContractResult(trx));
              processPrintln("");
              processStdin.flush();

              break;
            case ContractType.DelegateResourceContract_VALUE:
              BalanceContract.DelegateResourceContract drc = any.unpack(BalanceContract.DelegateResourceContract.class);
              address = drc.getReceiverAddress().toByteArray();
              priv = getTargetAddress(address);
              if (priv != null) {
                logTransactionCaptured(tp, address, "delegate_resource");
                processPrintln("type=delegate_resource");
                processPrintln("resource=" + drc.getResource().name());
                processPrintln("to=" + Hex.toHexString(address));
                processPrintln("priv=" + Hex.toHexString(priv));
                processPrintln("amount=" + drc.getBalance());
                processPrintln("txid=" + getTxId(trx));
                processPrintln("contractResult=" + getContractResult(trx));
                processPrintln("");
                processStdin.flush();
              }
              break;
            case ContractType.WithdrawBalanceContract_VALUE:
              BalanceContract.WithdrawBalanceContract wbc = any.unpack(BalanceContract.WithdrawBalanceContract.class);
              address = wbc.getOwnerAddress().toByteArray();
              priv = getTargetAddress(address);
              if (priv != null) {
                logTransactionCaptured(tp, address, "withdrawbalancecontract");
                processPrintln("type=withdrawbalancecontract");
                processPrintln("to=" + Hex.toHexString(address));
                processPrintln("priv=" + Hex.toHexString(priv));
                processPrintln("txid=" + getTxId(trx));
                processPrintln("contractResult=" + getContractResult(trx));
                processPrintln("");
                processStdin.flush();
              }
              break;
            case ContractType.WithdrawExpireUnfreezeContract_VALUE:
              BalanceContract.WithdrawExpireUnfreezeContract weuc = any.unpack(BalanceContract.WithdrawExpireUnfreezeContract.class);
              address = weuc.getOwnerAddress().toByteArray();
              priv = getTargetAddress(address);
              if (priv != null) {
                logTransactionCaptured(tp, address, "withdrawexpireunfreezecontract");
                processPrintln("type=withdrawexpireunfreezecontract");
                processPrintln("to=" + Hex.toHexString(address));
                processPrintln("priv=" + Hex.toHexString(priv));
                processPrintln("txid=" + getTxId(trx));
                processPrintln("contractResult=" + getContractResult(trx));
                processPrintln("");
                processStdin.flush();
              }
              break;
            case ContractType.UnfreezeBalanceContract_VALUE:
              BalanceContract.UnfreezeBalanceContract ubc = any.unpack(BalanceContract.UnfreezeBalanceContract.class);
              address = ubc.getOwnerAddress().toByteArray();
              priv = getTargetAddress(address);
              if (priv != null) {
                logTransactionCaptured(tp, address, "unfreezebalancecontract");
                processPrintln("type=unfreezebalancecontract");
                processPrintln("resource=" + ubc.getResource().name());
                processPrintln("to=" + Hex.toHexString(address));
                processPrintln("priv=" + Hex.toHexString(priv));
                processPrintln("txid=" + getTxId(trx));
                processPrintln("contractResult=" + getContractResult(trx));
                processPrintln("");
                processStdin.flush();
              }
              break;
          }
          TransactionInfoCapsule tic = transactionRetStore.getTransactionInfo(getTxIdBytes(trx));
          if (tic != null) {
            for (InternalTransaction it : tic.getInstance().getInternalTransactionsList()) {
              if (it.getTransferToAddress() != null) {
                byte[] addr = it.getTransferToAddress().toByteArray();
                tracePrintf("Internal transaction to %s", Hex.toHexString(addr));
                priv = getTargetAddress(addr);
                if (priv != null) {
                  processPrintln("type=internal");
                  processPrintln("to="
                          + Hex.toHexString(addr));
                  processPrintln("priv=" + Hex.toHexString(priv));
                  for (CallValueInfo cvi : it.getCallValueInfoList()) {
                    processPrintln("value=" + cvi.getCallValue());
                    processPrintln("token=" + cvi.getTokenId());
                  }
                  processPrintln("txid=" + getTxId(trx));
                  processPrintln("");
                  processStdin.flush();
                }
              }
            }
          }
        } catch (Exception e) {
          if (this.trace.get() != null) e.printStackTrace(this.trace.get().w);
          logger.error("In transaction capture", e);
        } finally {
          traceFinish();
          this.trace.set(null);
        }
      } catch (InterruptedException ex) {
        return;
      } catch (Exception e) {
        logger.warn("In transaction capture", e);
      }
    }
  }

  private BigInteger unpackUint256(ByteString data, int offset) {
    byte[] bytes = new byte[32];
    data.substring(offset, offset + 32).copyTo(bytes, 0);
    return new BigInteger(bytes);
  }

  private byte[] unpackAddress(ByteString data, int offset) {
    byte[] r = new byte[21];
    offset += 12;
    for (int i=0; i<20; i++) {
      r[i + 1] = data.byteAt(offset + i);
    }
    r[0] = 0x41;
    return r;
  }

  private boolean captureThisContract(ByteString contractAddress) {
    for (byte[] addr: trc20Contracts) {
      if (equals(contractAddress, addr, 21)) {
        return true;
      }
    }
    return false;
  }

  private static boolean equals(ByteString a, byte[] b, int l) {
    if (a.size() < l || b.length < l)
      return false;
    for (int i=0; i<l; i++) {
      if (a.byteAt(i) != b[i]) {
        return false;
      }
    }
    return true;
  }

  private long getLastChangeTimestamp() {
    final long[] l = new long[1];
    l[0] = scriptDirLastModified;

    for (String pattern: patterns) {
      try {
        Files.newDirectoryStream(scriptDir.toPath(), pattern)
                .forEach(path -> {
                  long lm = path.toFile().lastModified();
                  if (lm > l[0]) l[0] = lm;
                });
      } catch (IOException e) {
        logger.error("Error scanning " + pattern, e);
      }
    }
    return l[0];
  }

  private void checkScryptChanged() throws InterruptedException {
    long l = getLastChangeTimestamp();
    if (scriptDirLastModified < l) {
      scriptDirLastModified = l;
      logger.info("script directory changed; restarting the process");
      shutdownProcess();
      while (true) {
        try {
          createProcess();
          break;
        } catch (RuntimeException re) {
          Thread.sleep(1000);
        }
      }
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

    Options options = new Options().compressionType(CompressionType.NONE).createIfMissing(false)
            .verifyChecksums(false);
    try {
      db = new DB(dirFile, options);
      logger.info("Database opened: " + dirFile.getAbsolutePath());
    } catch (IOException e) {
      logger.error("Can't open " + dirFile.getAbsolutePath() + ": " + e);
      return;
    }

    // read and compose the bloom filter
    File counterFile = new File(dirFile, "COUNTER");
    long naddresses;
    try {
      FileReader counterFileReader = new FileReader(counterFile);
      BufferedReader br = new BufferedReader(counterFileReader);
      naddresses = Long.parseLong(br.readLine());
    } catch (IOException e) {
      logger.error("Can't open " + counterFile.getAbsolutePath() + ": " + e);
      return;
    }
    File bitmapFile = new File(dirFile, "BITMAP0");
    if (!bitmapFile.exists()) {
      logger.error("Bloom filter file not found: " + bitmapFile.getAbsolutePath());
      return;
    }
    long bsz = bitmapFile.length();

    if (bsz >= 0x80000000L) {
      logger.error("Bloom filters larger than 2047 MB are not supported");
      return;
    }

    // compute optimal number of hash functions, not larger than 4
    bloomHashes = (int)Math.round(bsz*8.0/naddresses * Math.log(2.0));
    if (bloomHashes < 1) bloomHashes = 1;
    else if (bloomHashes > 4) bloomHashes = 4;

    // join bitmaps into a Bloom filter
    bloom = new byte[(int)bsz];
    for (int bm = 0; bm < bloomHashes; bm++) {
      bitmapFile = new File(dirFile, "BITMAP"+bm);
      if (bitmapFile.length() != bsz) {
        logger.error("Inconsistent bitmap size in " + bitmapFile.getAbsolutePath() + ": " + bitmapFile.length());
        return;
      }
      int bufSize = 1024 * 1024;
      byte[] buf = new byte[bufSize];
      int offs = 0;
      try (FileInputStream fis = new FileInputStream(bitmapFile)) {
        while (offs < bsz) {
          fis.read(buf);
          for (int i = 0; i < bufSize; i++) {
            bloom[offs + i] |= buf[i];
          }
          offs += bufSize;
        }
      } catch (FileNotFoundException e) {
        logger.error("Bitmap file is missing: " + bitmapFile.getAbsolutePath());
        return;
      } catch (IOException e) {
        logger.error("Cannot open "+ bitmapFile.getAbsolutePath(), e);
        return;
      }
    }

    if (props.getProperty("benchmark", "false").equals("true")) {
      logger.info("Running benchmark on " + dirFile.getAbsolutePath());
      long start = System.currentTimeMillis();
      byte[] key = new byte[21];

      Random random = new Random();
      int n = 1000000;
      for (int i = 0; i < n; i++) {
        random.nextBytes(key);
        getTargetAddress(key);
      }
      logger.info("Fetch benchmark, keys/s: {}", n * 1000.0 / (System.currentTimeMillis() - start));

    }

    for (String addr: props.getProperty("trc20tokens", "").split(" ")) {
      trc20Contracts.add(Commons.decodeFromBase58Check(addr));
    }

    scriptDir = new File(props.getProperty("script_dir", "."));

    if (!scriptDir.isDirectory()) {
      logger.error("Directory " + scriptDir.getAbsolutePath() +
              " does not exist or not a directory; won't capture transactions");
      return;
    }

    patterns = props.getProperty("watch.files", "").split(";");

    scriptDirLastModified = getLastChangeTimestamp();

    commandLine = props.getProperty("script");

    if (commandLine == null) {
      logger.warn("No script property set in capture.props; won't capture transactions");
      return;
    }

    try {
      createProcess();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          checkScryptChanged();
        } catch (InterruptedException e) {
          timer.cancel();
        }
      }
    }, 10, 1000);

    startTraceService();
  }

  ServerSocket ss;
  Thread traceThread;

  void startTraceService()
  {
    try {
      ss = new ServerSocket(333);
      traceThread = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            Socket s = ss.accept();
            InputStreamReader rdr = new InputStreamReader(s.getInputStream());
            BufferedReader br = new BufferedReader(rdr);
            PrintWriter wr = new PrintWriter(s.getOutputStream());
            try {
              String line = br.readLine();
              while (br.readLine().length() != 0) ;

              String[] parts = line.split(" ");
              if (parts[0].equalsIgnoreCase("get")) {
                String[] params = parts[1].substring(1).split("/"); // remove /
                byte[] key = hexString2ByteString(params[0]).toByteArray();
                if (key.length != 32) {
                  wr.write("HTTP/1.1 500 Bad request\r\nContent-Type: text/plain\r\n\r\nTransaction should contain 32 bytes\r\n");
                } else {
                  TransactionCapsule tc = transactionStore.get(key);
                  wr.write("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nTry capture transaction " + tc + "\r\n");
                  PrintWriterWithSignal ws = new PrintWriterWithSignal(wr);
                  if (params.length == 2 && params[1].equals("force"))
                    ws.force = true;
                  trace.set(ws);
                  try {
                    capture(tc.getInstance(), false);
                  } catch (Exception ex) {
                    wr.write("Exception: " + ex + "\r\n");
                    ex.printStackTrace(wr);
                  } finally {
                    trace.set(null);
                  }
                  ws.s.await();
                }
              } else {
                wr.write("HTTP/1.1 500 Bad request\r\nContent-Type: text/plain\r\n\r\nPlease GET requests only\r\n");
              }
            } catch (Exception e) {
              wr.write("HTTP/1.1 500 Bad request\r\nContent-Type: text/plain\r\n\r\n");
              e.printStackTrace(wr);
            } finally {
              wr.close();
              s.close();
            }
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        } // while
      }, "trace service");
      traceThread.start();
    } catch (IOException e) {
      logger.error("Cannot start capture trace service: " + e.getMessage());
    }
  }

  void stopTraceService() {
    traceThread.interrupt();
    try {
      ss.close();
    } catch (IOException e) {
    }
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

  private synchronized void createProcess() throws InterruptedException {
    if (scriptThread != null) {
      scriptThread.interrupt();
      scriptThread = null;
    }

    try {
      List<String> cmd = parseCommandLine(commandLine);
      captureProcess = new ProcessBuilder()
              .command(cmd)
              .directory(scriptDir)
              .start();

      logger.debug("Process started: " + cmd);

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

      scriptThread.start();

      logger.debug("Threads started");
    } catch (InterruptedIOException iie) {
      throw new InterruptedException();
    } catch (IOException e) {
      logger.error("Creating a process: ", e);
      throw new RuntimeException(e);
    }
  }
}
