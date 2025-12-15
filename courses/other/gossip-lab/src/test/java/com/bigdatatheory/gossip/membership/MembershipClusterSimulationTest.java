package com.bigdatatheory.gossip.membership;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MembershipClusterSimulationTest {

    @Test
    public void testFailureLeadsToDeadStatus() {
        MembershipClusterSimulation simulation = new MembershipClusterSimulation(5, 123L, 3, 6);
        String failedNodeId = "node-2";
        int totalRounds = 20;
        simulation.runWithFailure(failedNodeId, 5, totalRounds);
        int round = simulation.getCurrentRound();
        for (Map.Entry<String, MembershipNode> entry : simulation.getNodesView().entrySet()) {
            String nodeId = entry.getKey();
            MembershipNode node = entry.getValue();
            MembershipStatus failedStatus = node.getStatus(failedNodeId, round);
            if (!nodeId.equals(failedNodeId)) {
                MembershipStatus selfStatus = node.getStatus(nodeId, round);
                Assert.assertEquals(MembershipStatus.ALIVE, selfStatus);
            }
            Assert.assertEquals(MembershipStatus.DEAD, failedStatus);
        }
    }
}
