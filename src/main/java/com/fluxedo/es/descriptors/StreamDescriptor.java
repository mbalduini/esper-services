package com.fluxedo.es.descriptors;

import com.espertech.esper.client.EventType;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Marco Balduini on 19/06/2018 as part of project esperservices.
 */
public class StreamDescriptor {

    private String cepURI;
    private String name;
    private String type;
    private ArrayList<TypeDescriptor> fields = new ArrayList<>();

    public StreamDescriptor() {
    }

    public StreamDescriptor(String cepURI, EventType et) {
        this.cepURI = cepURI;
        this.name = et.getName();
        this.type = et.getUnderlyingType().getTypeName();
    }

    public StreamDescriptor(String cepURI, String name, String type) {
        this.cepURI = cepURI;
        this.name = name;
        this.type = type;
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ArrayList<TypeDescriptor> getFields() {
        return fields;
    }

    public void setFields(ArrayList<TypeDescriptor> fields) {
        this.fields = fields;
    }

    public void addField(String name, String type) {
        fields.add(new TypeDescriptor(name,type));
    }

}
