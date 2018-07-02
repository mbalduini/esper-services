package com.fluxedo.es.descriptors;

import com.espertech.esper.client.EventType;

/**
 * Created by Marco Balduini on 19/06/2018 as part of project esperservices.
 */
public class StreamDescriptor {
    private String name;
    private String type;

    public StreamDescriptor() {
    }

    public StreamDescriptor(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public StreamDescriptor(EventType et) {
        this.name = et.getName();
        this.type = et.getUnderlyingType().getTypeName();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
