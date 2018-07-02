package com.fluxedo.es.descriptors;

import com.espertech.esper.client.EPStatement;
import com.fluxedo.es.internalPurposeDescriptor.ConsumerDescriptorIP;

import java.util.HashMap;

/**
 * Created by Marco Balduini on 19/06/2018 as part of project esperservices.
 */
public class QueryDescriptor {
    private String name;
    private String body;
    private String state;
    private HashMap <String, ConsumerDescriptor> consumers = new HashMap<>();

    public QueryDescriptor(String name, String body, String state, HashMap <String, ConsumerDescriptor> consumers) {
        this.name = name;
        this.body = body;
        this.state = state;
        this.consumers = consumers;
    }

    public QueryDescriptor(EPStatement eps) {
        this.name = eps.getName();
        this.body = eps.getText();
        this.state = eps.getState().toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public HashMap <String, ConsumerDescriptor> getConsumers() {
        return consumers;
    }

    public void setConsumers(HashMap <String, ConsumerDescriptor> consumers) {
        this.consumers = consumers;
    }

    public void addConsumer(ConsumerDescriptor cd4s){
        consumers.put(cd4s.getId(), cd4s);
    }

    public void removeConsumer(ConsumerDescriptor cd4s){
        consumers.remove(cd4s.getId());
    }
}
