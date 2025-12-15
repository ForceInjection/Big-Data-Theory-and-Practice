package com.bigdatatheory.gossip.membership;

import java.util.Map;

public final class MembershipSimulationMain {

    public static void main(String[] args) {
        int nodeCount = 5;
        int totalRounds = 20;
        int failedIndex = 2;
        int failureRound = 5;
        int suspectThreshold = 3;
        int deadThreshold = 6;
        if (args.length >= 1) {
            nodeCount = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            totalRounds = Integer.parseInt(args[1]);
        }
        if (args.length >= 3) {
            failedIndex = Integer.parseInt(args[2]);
        }
        if (args.length >= 4) {
            failureRound = Integer.parseInt(args[3]);
        }
        if (args.length >= 5) {
            suspectThreshold = Integer.parseInt(args[4]);
        }
        if (args.length >= 6) {
            deadThreshold = Integer.parseInt(args[5]);
        }
        String failedNodeId = "node-" + failedIndex;
        MembershipClusterSimulation simulation = new MembershipClusterSimulation(nodeCount, 42L, suspectThreshold, deadThreshold);
        simulation.runWithFailure(failedNodeId, failureRound, totalRounds);
        int snapshotRound = simulation.getCurrentRound();
        System.out.println("failedNode=" + failedNodeId + " failureRound=" + failureRound + " totalRounds=" + totalRounds + " suspectThreshold=" + suspectThreshold + " deadThreshold=" + deadThreshold);
        for (Map.Entry<String, MembershipNode> entry : simulation.getNodesView().entrySet()) {
            String nodeId = entry.getKey();
            MembershipNode node = entry.getValue();
            StringBuilder builder = new StringBuilder();
            builder.append(nodeId).append(" view:");
            for (String memberId : node.getMembersSnapshot()) {
                MembershipStatus status = node.getStatus(memberId, snapshotRound);
                builder.append(" ").append(memberId).append("=").append(status.name());
            }
            System.out.println(builder.toString());
        }
    }
}
