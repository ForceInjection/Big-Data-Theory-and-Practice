package com.bigdatatheory.gossip.simulation;

import com.bigdatatheory.gossip.core.GossipMessage;
import com.bigdatatheory.gossip.core.GossipNode;
import com.bigdatatheory.gossip.core.GossipTransport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public final class GossipClusterSimulation implements GossipTransport {

    private final Map<String, GossipNode> nodes;
    private final Random random;

    public GossipClusterSimulation(int nodeCount, long randomSeed) {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("nodeCount must be positive");
        }
        this.random = new Random(randomSeed);
        this.nodes = new HashMap<>();

        List<String> nodeIds = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodeIds.add("node-" + i);
        }
        Set<String> initialMembers = new HashSet<>(nodeIds);

        for (String nodeId : nodeIds) {
            GossipNode node = new GossipNode(nodeId, this, initialMembers, new Random(random.nextLong()));
            nodes.put(nodeId, node);
        }
    }

    public Map<String, GossipNode> getNodesView() {
        return Collections.unmodifiableMap(nodes);
    }

    public void runRounds(int rounds) {
        if (rounds < 0) {
            throw new IllegalArgumentException("rounds must be non-negative");
        }
        for (int round = 0; round < rounds; round++) {
            for (GossipNode node : nodes.values()) {
                node.tick();
            }
        }
    }

    @Override
    public void send(String from, String to, GossipMessage message) {
        GossipNode target = nodes.get(to);
        if (target == null) {
            return;
        }
        target.onReceive(message);
    }
}

