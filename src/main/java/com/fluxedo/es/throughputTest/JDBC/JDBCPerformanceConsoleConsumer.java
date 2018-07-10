package com.fluxedo.es.throughputTest.JDBC;

import com.espertech.esper.client.EventBean;
import com.fluxedo.es.commons.Consumer;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class JDBCPerformanceConsoleConsumer extends Consumer {

    private long totalConsumedMessages;

    public JDBCPerformanceConsoleConsumer() {
        super();
    }

    public void initialize(String configuration){

    }

    public void update(EventBean[] newData, EventBean[] oldData) {

        for (EventBean e : newData) {
            e.getUnderlying().toString();
//            for ( String s : e.getEventType().getPropertyNames()) {
//                System.out.println(s + " : " + e.get(s).toString());
//            }
//            System.out.println();
//            System.out.println("Plus other " + (newData.length - 1) + " similar events");
        }
        totalConsumedMessages = totalConsumedMessages + newData.length;
    }

    public long getTotalConsumedMessages() {
        return totalConsumedMessages;
    }
    public void truncateConsumedMessages() {
        totalConsumedMessages = 0;
    }
}
