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

public final class PartitionedMembershipClusterSimulation implements GossipTransport {

    private final Map<String, MembershipNode> nodes;
    private final Random random;
    private final int suspectThreshold;
    private final int deadThreshold;
    private final int partitionStartRound;
    private final int partitionEndRound;
    private final double crossDropProbability;
    private int currentRound;

    public PartitionedMembershipClusterSimulation(int nodeCount, long randomSeed, int suspectThreshold, int deadThreshold, int partitionStartRound, int partitionEndRound, double crossDropProbability) {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("nodeCount must be positive");
        }
        if (suspectThreshold <= 0 || deadThreshold <= suspectThreshold) {
            throw new IllegalArgumentException("thresholds must satisfy 0 < suspect < dead");
        }
        if (partitionStartRound <= 0 || partitionEndRound < partitionStartRound) {
            throw new IllegalArgumentException("partition rounds must satisfy 0 < start <= end");
        }
        if (crossDropProbability < 0.0 || crossDropProbability > 1.0) {
            throw new IllegalArgumentException("crossDropProbability must be between 0.0 and 1.0");
        }
        this.random = new Random(randomSeed);
        this.nodes = new HashMap<>();
        this.suspectThreshold = suspectThreshold;
        this.deadThreshold = deadThreshold;
        this.partitionStartRound = partitionStartRound;
        this.partitionEndRound = partitionEndRound;
        this.crossDropProbability = crossDropProbability;
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

    public void run(int totalRounds) {
        if (totalRounds < 0) {
            throw new IllegalArgumentException("totalRounds must be non-negative");
        }
        for (int round = 1; round <= totalRounds; round++) {
            currentRound = round;
            for (MembershipNode node : nodes.values()) {
                node.tick(currentRound);
            }
        }
    }

    @Override
    public void send(String from, String to, GossipMessage message) {
        boolean crossPartition = isCrossPartition(from, to);
        if (currentRound >= partitionStartRound && currentRound <= partitionEndRound && crossPartition) {
            if (random.nextDouble() < crossDropProbability) {
                return;
            }
        }
        MembershipNode target = nodes.get(to);
        if (target == null) {
            return;
        }
        target.onReceive(message, currentRound);
    }

    private boolean isCrossPartition(String from, String to) {
        int fromIndex = parseIndex(from);
        int toIndex = parseIndex(to);
        boolean fromGroup = fromIndex < nodes.size() / 2;
        boolean toGroup = toIndex < nodes.size() / 2;
        return fromGroup != toGroup;
    }

    private int parseIndex(String nodeId) {
        if (nodeId == null || !nodeId.startsWith("node-")) {
            return 0;
        }
        String suffix = nodeId.substring("node-".length());
        try {
            return Integer.parseInt(suffix);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}

