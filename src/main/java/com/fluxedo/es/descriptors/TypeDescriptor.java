package com.fluxedo.es.descriptors;

/**
 * Created by Marco Balduini on 06/07/2018 as part of project esperservices.
 */
public class TypeDescriptor {
    private String name;
    private String type;

    public TypeDescriptor() {
    }

    public TypeDescriptor(String name, String type) {
        this.name = name;
        this.type = type;
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
