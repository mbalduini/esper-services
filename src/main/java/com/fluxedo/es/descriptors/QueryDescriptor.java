package com.fluxedo.es.descriptors;

import com.espertech.esper.client.EPStatement;
import com.fluxedo.es.internalPurposeDescriptor.ConsumerDescriptorIP;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Marco Balduini on 19/06/2018 as part of project esperservices.
 */
public class QueryDescriptor {

    private String cepURI;
    private String name;
    private String body;
    private String state;
    private ArrayList<ConsumerDescriptor> consumers = new ArrayList<>();

    public QueryDescriptor(String cepURI, EPStatement eps) {
        this.cepURI = cepURI;
        this.name = eps.getName();
        this.body = eps.getText();
        this.state = eps.getState().toString();
    }

    public QueryDescriptor(String cepURI, String name, String body, String state, ArrayList<ConsumerDescriptor> consumers) {
        this.cepURI = cepURI;
        this.name = name;
        this.body = body;
        this.state = state;
        this.consumers = consumers;
    }

    public String getCepURI() {
        return cepURI;
    }

    public void setCepURI(String cepURI) {
        this.cepURI = cepURI;
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

    public ArrayList<ConsumerDescriptor> getConsumers() {
        return consumers;
    }

    public void setConsumers(ArrayList<ConsumerDescriptor> consumers) {
        this.consumers = consumers;
    }

    public void addConsumer(ConsumerDescriptor cd4s){
        consumers.add(cd4s);
    }

    public void removeConsumer(ConsumerDescriptor cd4s){
        consumers.remove(cd4s);
    }
}
