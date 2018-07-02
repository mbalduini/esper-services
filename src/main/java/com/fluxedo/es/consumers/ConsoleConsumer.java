package com.fluxedo.es.consumers;

import com.espertech.esper.client.EventBean;
import com.fluxedo.es.commons.Consumer;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class ConsoleConsumer extends Consumer {

    public ConsoleConsumer() {
        super();
    }

    public void initialize(String configuration){

    }

    public void update(EventBean[] newData, EventBean[] oldData) {

        for (EventBean e : newData) {
            System.out.println(e.getUnderlying().toString());
            break;

//            if (e instanceof MapEventBean) {
//                MapEventBean meb = (MapEventBean) e;
//
//                Map<String, Object> m = meb.getProperties();
//
//                for (Map.Entry entry : m.entrySet()) {
//                    System.out.println(entry.getKey() + "," + entry.getValue().toString());
//                }
//            }  else if (e instanceof ObjectArrayEventBean){
//                ObjectArrayEventBean oaeb = (ObjectArrayEventBean) e;
//                StringBuffer sb = new StringBuffer();
//                for (Object o : oaeb.getProperties()){
//                    sb.append(o.toString() + ",");
//                }
//                sb.delete(sb.lastIndexOf(","), sb.length());
//                System.out.println(sb.toString());
//            } else {
//                System.out.println(e.getUnderlying().toString());
//            }
        }
    }
}
