package com.fluxedo.es.internalPurposeDescriptor;

import com.espertech.esper.client.EPServiceProvider;
import com.fluxedo.es.descriptors.JDBCConnectionDescriptor;

import java.util.ArrayList;

/**
 * Created by Marco Balduini on 03/07/2018 as part of project esperservices.
 */
public class CEPDescriptorIP {

    private EPServiceProvider cep;
    private String URI;
    private ArrayList<JDBCConnectionDescriptor> JDBCConnections = new ArrayList<>();

    public CEPDescriptorIP() {
    }

    public CEPDescriptorIP(EPServiceProvider cep, String URI, ArrayList<JDBCConnectionDescriptor> JDBCConnections) {
        this.cep = cep;
        this.URI = URI;
        this.JDBCConnections = JDBCConnections;
    }

    public EPServiceProvider getCep() {
        return cep;
    }

    public void setCep(EPServiceProvider cep) {
        this.cep = cep;
    }

    public String getURI() {
        return URI;
    }

    public void setURI(String URI) {
        this.URI = URI;
    }

    public ArrayList<JDBCConnectionDescriptor> getJDBCConnections() {
        return JDBCConnections;
    }

    public void setJDBCConnections(ArrayList<JDBCConnectionDescriptor> JDBCConnections) {
        this.JDBCConnections = JDBCConnections;
    }

    public void addJDBCConnection(JDBCConnectionDescriptor jd){
        JDBCConnections.add(jd);
    }
}
