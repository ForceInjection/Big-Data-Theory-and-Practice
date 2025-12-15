package com.bigdatatheory.gossip.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class GossipMessage {

    private final String from;
    private final Map<String, Long> heartbeatView;

    public GossipMessage(String from, Map<String, Long> heartbeatView) {
        this.from = from;
        this.heartbeatView = Collections.unmodifiableMap(new HashMap<>(heartbeatView));
    }

    public String getFrom() {
        return from;
    }

    public Map<String, Long> getHeartbeatView() {
        return heartbeatView;
    }
}

