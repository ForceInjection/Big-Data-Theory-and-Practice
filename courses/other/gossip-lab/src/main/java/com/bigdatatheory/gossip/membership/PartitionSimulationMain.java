package com.bigdatatheory.gossip.membership;

import java.util.Map;

public final class PartitionSimulationMain {

    public static void main(String[] args) {
        int nodeCount = 6;
        int totalRounds = 20;
        int partitionStartRound = 5;
        int partitionEndRound = 20;
        int suspectThreshold = 3;
        int deadThreshold = 6;
        double crossDropProbability = 1.0;
        if (args.length >= 1) {
            nodeCount = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            totalRounds = Integer.parseInt(args[1]);
        }
        if (args.length >= 3) {
            partitionStartRound = Integer.parseInt(args[2]);
        }
        if (args.length >= 4) {
            partitionEndRound = Integer.parseInt(args[3]);
        }
        if (args.length >= 5) {
            suspectThreshold = Integer.parseInt(args[4]);
        }
        if (args.length >= 6) {
            deadThreshold = Integer.parseInt(args[5]);
        }
        if (args.length >= 7) {
            crossDropProbability = Double.parseDouble(args[6]);
        }
        PartitionedMembershipClusterSimulation simulation = new PartitionedMembershipClusterSimulation(
                nodeCount,
                99L,
                suspectThreshold,
                deadThreshold,
                partitionStartRound,
                partitionEndRound,
                crossDropProbability
        );
        simulation.run(totalRounds);
        int round = simulation.getCurrentRound();
        System.out.println("partitioned simulation: nodeCount=" + nodeCount + " totalRounds=" + totalRounds + " partition=[" + partitionStartRound + "," + partitionEndRound + "] suspectThreshold=" + suspectThreshold + " deadThreshold=" + deadThreshold + " crossDropProbability=" + crossDropProbability + " snapshotRound=" + round);
        for (Map.Entry<String, MembershipNode> entry : simulation.getNodesView().entrySet()) {
            String nodeId = entry.getKey();
            MembershipNode node = entry.getValue();
            StringBuilder builder = new StringBuilder();
            builder.append(nodeId).append(" view:");
            for (String memberId : node.getMembersSnapshot()) {
                MembershipStatus status = node.getStatus(memberId, round);
                builder.append(" ").append(memberId).append("=").append(status.name());
            }
            System.out.println(builder.toString());
        }
    }
}

