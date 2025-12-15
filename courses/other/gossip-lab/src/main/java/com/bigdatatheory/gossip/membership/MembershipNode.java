package com.bigdatatheory.gossip.membership;

import com.bigdatatheory.gossip.core.GossipMessage;
import com.bigdatatheory.gossip.core.GossipTransport;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public final class MembershipNode {

    private final String nodeId;
    private final GossipTransport transport;
    private final Random random;
    private final Set<String> members;
    private final Map<String, Long> heartbeat;
    private final Map<String, Integer> lastSeenRound;
    private final int suspectThreshold;
    private final int deadThreshold;

    public MembershipNode(String nodeId, GossipTransport transport, Set<String> initialMembers, Random random, int suspectThreshold, int deadThreshold) {
        this.nodeId = nodeId;
        this.transport = transport;
        this.random = random;
        this.members = new HashSet<>(initialMembers);
        this.heartbeat = new HashMap<>();
        this.lastSeenRound = new HashMap<>();
        this.suspectThreshold = suspectThreshold;
        this.deadThreshold = deadThreshold;
        for (String memberId : initialMembers) {
            lastSeenRound.put(memberId, 0);
        }
        heartbeat.put(nodeId, 0L);
    }

    public String getNodeId() {
        return nodeId;
    }

    public Set<String> getMembersSnapshot() {
        return Collections.unmodifiableSet(new HashSet<>(members));
    }

    public Map<String, Long> getHeartbeatSnapshot() {
        return Collections.unmodifiableMap(new HashMap<>(heartbeat));
    }

    public void tick(int currentRound) {
        long next = heartbeat.getOrDefault(nodeId, 0L) + 1L;
        heartbeat.put(nodeId, next);
        lastSeenRound.put(nodeId, currentRound);
        String target = chooseRandomPeer();
        if (target == null) {
            return;
        }
        GossipMessage message = new GossipMessage(nodeId, heartbeat);
        transport.send(nodeId, target, message);
    }

    public void onReceive(GossipMessage message, int currentRound) {
        Map<String, Long> incoming = message.getHeartbeatView();
        for (Map.Entry<String, Long> entry : incoming.entrySet()) {
            String memberId = entry.getKey();
            Long remoteHeartbeat = entry.getValue();
            Long localHeartbeat = heartbeat.get(memberId);
            if (localHeartbeat == null || remoteHeartbeat > localHeartbeat) {
                heartbeat.put(memberId, remoteHeartbeat);
                lastSeenRound.put(memberId, currentRound);
            }
            members.add(memberId);
        }
    }

    public MembershipStatus getStatus(String memberId, int currentRound) {
        Integer lastSeen = lastSeenRound.get(memberId);
        if (lastSeen == null) {
            return MembershipStatus.SUSPECT;
        }
        int delta = currentRound - lastSeen;
        if (delta <= suspectThreshold) {
            return MembershipStatus.ALIVE;
        }
        if (delta <= deadThreshold) {
            return MembershipStatus.SUSPECT;
        }
        return MembershipStatus.DEAD;
    }

    private String chooseRandomPeer() {
        List<String> peers = members.stream()
                .filter(id -> !id.equals(nodeId))
                .collect(Collectors.toList());
        if (peers.isEmpty()) {
            return null;
        }
        int index = random.nextInt(peers.size());
        return peers.get(index);
    }
}
