package com.fluxedo.es.descriptors;

import java.util.ArrayList;

/**
 * Created by Marco Balduini on 03/07/2018 as part of project esperservices.
 */
public class CEPDescriptor {

    private String URI;
    private ArrayList<JDBCConnectionDescriptor> JDBCConnections = new ArrayList<>();

    public CEPDescriptor() {
    }

    public CEPDescriptor(String URI, ArrayList<JDBCConnectionDescriptor> JDBCConnections) {
        this.URI = URI;
        this.JDBCConnections = JDBCConnections;
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
