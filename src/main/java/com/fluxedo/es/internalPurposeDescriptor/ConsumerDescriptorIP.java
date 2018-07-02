package com.fluxedo.es.internalPurposeDescriptor;

import com.fluxedo.es.commons.Consumer;

/**
 * Created by Marco Balduini on 02/07/2018 as part of project esperservices.
 */
public class ConsumerDescriptorIP {

    private Consumer consumer;
    private String id;
    private String EPLQueryName;
    private String type;

    public ConsumerDescriptorIP() {
    }

    public ConsumerDescriptorIP(Consumer consumer, String id, String EPLQueryName, String type) {
        this.consumer = consumer;
        this.id = id;
        this.EPLQueryName = EPLQueryName;
        this.type = type;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
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
