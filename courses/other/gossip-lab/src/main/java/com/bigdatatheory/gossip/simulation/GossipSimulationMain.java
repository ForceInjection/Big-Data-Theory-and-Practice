package com.bigdatatheory.gossip.simulation;

import com.bigdatatheory.gossip.core.GossipNode;

import java.util.Map;

public final class GossipSimulationMain {

    public static void main(String[] args) {
        int nodeCount = 5;
        int rounds = 20;
        if (args.length >= 1) {
            nodeCount = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            rounds = Integer.parseInt(args[1]);
        }

        GossipClusterSimulation simulation = new GossipClusterSimulation(nodeCount, 42L);
        simulation.runRounds(rounds);

        for (Map.Entry<String, GossipNode> entry : simulation.getNodesView().entrySet()) {
            String nodeId = entry.getKey();
            GossipNode node = entry.getValue();
            int memberCount = node.getMembersSnapshot().size();
            long maxHeartbeat = node.getHeartbeatSnapshot().values().stream()
                    .mapToLong(Long::longValue)
                    .max()
                    .orElse(0L);
            System.out.println(nodeId + " members=" + memberCount + " maxHeartbeat=" + maxHeartbeat);
        }
    }
}

