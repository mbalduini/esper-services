package com.fluxedo.es.internalPurposeDescriptor;

import com.espertech.esper.client.EventType;
import com.fluxedo.es.commons.StreamManager;

import java.util.stream.Stream;

/**
 * Created by Marco Balduini on 19/06/2018 as part of project esperservices.
 */
public class StreamDescriptorIP {
    private StreamManager sm;

    public StreamDescriptorIP() {
    }

    public StreamDescriptorIP(StreamManager sm) {
        this.sm = sm;
    }

    public StreamManager getSm() {
        return sm;
    }

    public void setSm(StreamManager sm) {
        this.sm = sm;
    }
}
