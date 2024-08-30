package org.tron.core.store;

import com.google.common.collect.Streams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.ProposalCapsule;
import org.tron.core.db.TronStoreWithRevoking;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.protos.Protocol.Proposal.State;

@Component
public class ProposalStore extends TronStoreWithRevoking<ProposalCapsule> {

  @Autowired
  public ProposalStore(@Value("proposal") String dbName) {
    super(dbName);
  }

  @Override
  public ProposalCapsule get(byte[] key) throws ItemNotFoundException {
    byte[] value = revokingDB.get(key);
    return new ProposalCapsule(value);
  }

  /**
   * get all proposals.
   */
  public List<ProposalCapsule> getAllProposals() {
    return Streams.stream(iterator())
        .map(Map.Entry::getValue)
        .sorted(
            (ProposalCapsule a, ProposalCapsule b) -> a.getCreateTime() <= b.getCreateTime() ? 1
                : -1)
        .collect(Collectors.toList());
  }

  /**
   * note: return in asc order by expired time
   */
  public List<ProposalCapsule> getSpecifiedProposals(State state, long code) {
    return Streams.stream(iterator())
        .map(Map.Entry::getValue)
        .filter(proposalCapsule -> proposalCapsule.getState().equals(state))
        .filter(proposalCapsule -> proposalCapsule.getParameters().containsKey(code))
        .sorted(
            (ProposalCapsule a, ProposalCapsule b) -> a.getExpirationTime() > b.getExpirationTime()
                ? 1 : -1)
        .collect(Collectors.toList());
  }

  public void initGenesisProposals(BlockCapsule genesisBlock) {
    Map<Long, Long> parameters = new HashMap<>();
    parameters.put(9L, 1L);
    parameters.put(10L, 1L);
    parameters.put(11L, 420L);
    parameters.put(19L, 90000000000L);
    parameters.put(15L, 1L);
    parameters.put(18L, 1L);
    parameters.put(16L, 1L);
    parameters.put(20L, 1L);
    parameters.put(26L, 1L);
    parameters.put(30L, 1L);
    parameters.put(5L, 16000000L);
    parameters.put(31L, 160000000L);
    parameters.put(32L, 1L);
    parameters.put(39L, 1L);
    parameters.put(41L, 1L);
    parameters.put(3L, 1000L);
    parameters.put(47L, 10000000000L);
    parameters.put(49L, 1L);
    parameters.put(13L, 80L);
    parameters.put(7L, 1000000L);
    parameters.put(61L, 600L);
    parameters.put(63L, 1L);
    parameters.put(65L, 1L);
    parameters.put(66L, 1L);
    parameters.put(67L, 1L);
    parameters.put(68L, 1000000L);
    parameters.put(69L, 1L);
    parameters.put(52L, 1L); // allow TVM freeze
    parameters.put(70L, 365L); // UNFREEZE_DELAY_DAYS
    parameters.put(71L, 1L);
    parameters.put(76L, 1L);

    Map<Long, Long> parameters1 = new HashMap<>();
    parameters1.put(47L, 15000000000L);
    parameters1.put(59L, 1L);
    parameters1.put(72L, 1L);
    parameters1.put(73L, 3000000000L);
    parameters1.put(74L, 2000L);
    parameters1.put(75L, 12000L);
    parameters1.put(77L, 1L);
    parameters1.put(78L, 864000L);

    ByteString wit = genesisBlock.getWitnessAddress();
    putProposal(parameters, wit, 1);
    putProposal(parameters1, wit, 2);
  }

  private void putProposal(Map<Long, Long> parameters, ByteString wit, int proposalId) {
    ProposalCapsule proposalCapsule = new ProposalCapsule(wit, proposalId);
    proposalCapsule.setParameters(parameters);
    proposalCapsule.addApproval(wit);
    put(proposalCapsule.createDbKey(), proposalCapsule);
  }
}