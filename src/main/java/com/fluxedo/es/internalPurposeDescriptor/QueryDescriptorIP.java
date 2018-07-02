package com.fluxedo.es.internalPurposeDescriptor;

import com.espertech.esper.client.EPStatement;

import java.util.HashMap;

/**
 * Created by Marco Balduini on 19/06/2018 as part of project esperservices.
 */
public class QueryDescriptorIP {
    private EPStatement epStatement;
    private HashMap <String, ConsumerDescriptorIP> consumers = new HashMap<>();

    public QueryDescriptorIP(EPStatement epStatement) {
        this.epStatement = epStatement;
    }

    public QueryDescriptorIP(EPStatement eps, HashMap <String, ConsumerDescriptorIP> consumers) {
        this.epStatement = eps;
        this.consumers = consumers;

    }

    public EPStatement getEpStatement() {
        return epStatement;
    }

    public void setEpStatement(EPStatement epStatement) {
        this.epStatement = epStatement;
    }

    public HashMap <String, ConsumerDescriptorIP> getConsumers() {
        return consumers;
    }

    public void setConsumers(HashMap <String, ConsumerDescriptorIP> consumers) {
        this.consumers = consumers;
    }

    public void addConsumer(ConsumerDescriptorIP cd4s){
        consumers.put(cd4s.getId(), cd4s);
    }

    public void removeConsumer(ConsumerDescriptorIP cd4s){
        consumers.remove(cd4s.getId());
    }
}
