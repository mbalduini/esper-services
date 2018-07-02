package com.fluxedo.es.descriptors;

import com.espertech.esper.client.UpdateListener;
import com.fluxedo.es.commons.Consumer;

/**
 * Created by Marco Balduini on 02/07/2018 as part of project esperservices.
 */
public class ConsumerDescriptor {

    private String id;
    private String EPLQueryName;
    private String type;

    public ConsumerDescriptor() {
    }

    public ConsumerDescriptor(String id, String EPLQueryName, String type) {
        this.id = id;
        this.EPLQueryName = EPLQueryName;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEPLQueryName() {
        return EPLQueryName;
    }

    public void setEPLQueryName(String EPLQueryName) {
        this.EPLQueryName = EPLQueryName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
