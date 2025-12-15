package com.bigdatatheory.gossip.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public final class GossipNode {

    private final String nodeId;
    private final GossipTransport transport;
    private final Random random;
    private final Set<String> members;
    private final Map<String, Long> heartbeat;

    public GossipNode(String nodeId, GossipTransport transport, Set<String> initialMembers, Random random) {
        this.nodeId = nodeId;
        this.transport = transport;
        this.random = random;
        this.members = new HashSet<>(initialMembers);
        this.members.add(nodeId);
        this.heartbeat = new HashMap<>();
        this.heartbeat.put(nodeId, 0L);
    }

    public String getNodeId() {
        return nodeId;
    }

    public Map<String, Long> getHeartbeatSnapshot() {
        return Collections.unmodifiableMap(new HashMap<>(heartbeat));
    }

    public Set<String> getMembersSnapshot() {
        return Collections.unmodifiableSet(new HashSet<>(members));
    }

    public void tick() {
        long next = heartbeat.getOrDefault(nodeId, 0L) + 1L;
        heartbeat.put(nodeId, next);

        String target = chooseRandomPeer();
        if (target == null) {
            return;
        }

        GossipMessage message = new GossipMessage(nodeId, heartbeat);
        transport.send(nodeId, target, message);
    }

    public void onReceive(GossipMessage message) {
        Map<String, Long> incoming = message.getHeartbeatView();
        for (Map.Entry<String, Long> entry : incoming.entrySet()) {
            String memberId = entry.getKey();
            Long remoteHeartbeat = entry.getValue();
            Long localHeartbeat = heartbeat.get(memberId);
            if (localHeartbeat == null || remoteHeartbeat > localHeartbeat) {
                heartbeat.put(memberId, remoteHeartbeat);
            }
        }
        members.addAll(incoming.keySet());
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

