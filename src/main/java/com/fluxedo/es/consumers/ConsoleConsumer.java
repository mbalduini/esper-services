package com.fluxedo.es.consumers;

import com.espertech.esper.client.EventBean;
import com.fluxedo.es.commons.Consumer;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class ConsoleConsumer extends Consumer {

    private boolean sampling = true;

    public ConsoleConsumer() {
        super();
    }

    public void initialize(String configuration){
        Any config = JsonIterator.deserialize(configuration);
        sampling = config.get("sampling").toBoolean();
    }

    public void update(EventBean[] newData, EventBean[] oldData) {

        for (EventBean e : newData) {
            if(e.getUnderlying() != null) {
                for (String s : e.getEventType().getPropertyNames()) {
                    System.out.println(s + " : " + e.get(s).toString());
                }
            }
            if(sampling) {
                System.out.println();
                System.out.println("Plus other " + (newData.length - 1) + " similar events");
                System.out.println();
                break;
            }
        }
    }
}
