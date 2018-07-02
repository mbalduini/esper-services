package com.fluxedo.es.commons;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import java.util.UUID;

/**
 * Created by Marco Balduini on 28/06/2018 as part of project esperservices.
 */
public abstract class Consumer implements UpdateListener {

    private UUID id = UUID.randomUUID();

    public UUID getId(){
        return id;
    }

    public abstract void initialize(String configuration);
    public abstract void update(EventBean[] newData, EventBean[] oldData);

}
