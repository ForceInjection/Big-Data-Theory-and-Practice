package com.bigdatatheory.gossip.core;

public interface GossipTransport {

    void send(String from, String to, GossipMessage message);
}

