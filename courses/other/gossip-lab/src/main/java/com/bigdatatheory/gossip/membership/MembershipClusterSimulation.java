package com.bigdatatheory.gossip.membership;

import com.bigdatatheory.gossip.core.GossipMessage;
import com.bigdatatheory.gossip.core.GossipTransport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public final class MembershipClusterSimulation implements GossipTransport {

    private final Map<String, MembershipNode> nodes;
    private final Set<String> failedNodes;
    private final Random random;
    private final int suspectThreshold;
    private final int deadThreshold;
    private int currentRound;

    public MembershipClusterSimulation(int nodeCount, long randomSeed, int suspectThreshold, int deadThreshold) {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("nodeCount must be positive");
        }
        if (suspectThreshold <= 0 || deadThreshold <= suspectThreshold) {
            throw new IllegalArgumentException("thresholds must satisfy 0 < suspect < dead");
        }
        this.random = new Random(randomSeed);
        this.nodes = new HashMap<>();
        this.failedNodes = new HashSet<>();
        this.suspectThreshold = suspectThreshold;
        this.deadThreshold = deadThreshold;
        this.currentRound = 0;
        List<String> nodeIds = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodeIds.add("node-" + i);
        }
        Set<String> initialMembers = new HashSet<>(nodeIds);
        for (String nodeId : nodeIds) {
            MembershipNode node = new MembershipNode(nodeId, this, initialMembers, new Random(random.nextLong()), suspectThreshold, deadThreshold);
            nodes.put(nodeId, node);
        }
    }

    public Map<String, MembershipNode> getNodesView() {
        return Collections.unmodifiableMap(nodes);
    }

    public int getCurrentRound() {
        return currentRound;
    }

    public void runWithFailure(String nodeToFail, int failureRound, int totalRounds) {
        if (failureRound <= 0 || failureRound > totalRounds) {
            throw new IllegalArgumentException("failureRound must be between 1 and totalRounds");
        }
        if (!nodes.containsKey(nodeToFail)) {
            throw new IllegalArgumentException("nodeToFail not found: " + nodeToFail);
        }
        if (totalRounds < 0) {
            throw new IllegalArgumentException("totalRounds must be non-negative");
        }
        for (int round = 1; round <= totalRounds; round++) {
            currentRound = round;
            if (round == failureRound) {
                failedNodes.add(nodeToFail);
            }
            for (Map.Entry<String, MembershipNode> entry : nodes.entrySet()) {
                String nodeId = entry.getKey();
                MembershipNode node = entry.getValue();
                if (failedNodes.contains(nodeId)) {
                    continue;
                }
                node.tick(currentRound);
            }
        }
    }

    @Override
    public void send(String from, String to, GossipMessage message) {
        if (failedNodes.contains(to)) {
            return;
        }
        MembershipNode target = nodes.get(to);
        if (target == null) {
            return;
        }
        target.onReceive(message, currentRound);
    }
}

