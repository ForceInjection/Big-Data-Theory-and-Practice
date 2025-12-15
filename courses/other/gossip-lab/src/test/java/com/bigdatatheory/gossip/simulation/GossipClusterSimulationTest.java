package com.bigdatatheory.gossip.simulation;

import com.bigdatatheory.gossip.core.GossipNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class GossipClusterSimulationTest {

    @Test
    public void testHeartbeatConvergesToFullMembership() {
        GossipClusterSimulation simulation = new GossipClusterSimulation(5, 123L);
        simulation.runRounds(20);

        Map<String, GossipNode> nodes = simulation.getNodesView();
        for (GossipNode node : nodes.values()) {
            Assert.assertEquals(5, node.getMembersSnapshot().size());
            Assert.assertEquals(5, node.getHeartbeatSnapshot().size());
        }
    }
}

