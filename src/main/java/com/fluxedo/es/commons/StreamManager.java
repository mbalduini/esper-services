package com.fluxedo.es.commons;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventType;

/**
 * Created by Marco Balduini on 28/06/2018 as part of project esperservices.
 */
public abstract class StreamManager {

    public abstract void initialize(EPServiceProvider cep, String configuration);
    public abstract boolean createStream();
    public abstract EventType getStream();
}
