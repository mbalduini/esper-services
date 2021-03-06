package com.fluxedo.es.throughputTest.JDBC;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.fusesource.mqtt.client.*;
import org.fusesource.mqtt.client.MQTT;

import java.util.HashMap;

/**
 * Created by Marco Balduini on 20/03/2018 as part of project brembo_poc.
 */
public class JDBCPerformanceDataInjecterWithDataTransformation implements Runnable{

    private MQTT mqtt;
    private String topicId;
    private long totalConsumedMessages = 0;
    private Any config;
    private EPServiceProvider cep;

    public JDBCPerformanceDataInjecterWithDataTransformation(EPServiceProvider cep, MQTT mqtt, String configAsStr) {
        this.cep = cep;
        this.config = JsonIterator.deserialize(configAsStr);
        this.mqtt = mqtt;
        this.topicId = config.get("mqttConnectionInfo").get("topic").toString();
    }

    public void run() {

        //create connection and connect to the JDBC broker
        BlockingConnection thConnection = mqtt.blockingConnection();
        try {
            thConnection.connect();
            Topic[] topics = {new Topic(topicId, QoS.AT_LEAST_ONCE)};
            thConnection.subscribe(topics);
        } catch (Exception e) {
            e.printStackTrace();
        }

        EPRuntime cepRT = cep.getEPRuntime();

        //Start receiving message and push them in Esper
        while (true){
            try {
                Message message = thConnection.receive();
                String payload = new String(message.getPayload());
                totalConsumedMessages++;
                message.ack();

                Any tempEvent = JsonIterator.deserialize(payload);

                HashMap<String, Object> map;

                for (Any record : tempEvent.get("telemetryDataList").asList()) {
                    map = new HashMap<>();

                    for (Any field : config.get("dataSchema").asList()) {

                        switch (field.get("fieldType").toString().toLowerCase()) {
                            case "string":
                                try{
                                    Any a = record.get(field.get("fieldName").toString());
                                    a.mustBeValid();
                                    map.put(field.get("fieldName").toString(), a.toString());
                                } catch (Exception e){
                                    try {
                                        Any a = tempEvent.get(field.get("fieldName").toString());
                                        a.mustBeValid();
                                        map.put(field.get("fieldName").toString(), a.toString());
                                    } catch (Exception ex){
                                        map.put(field.get("fieldName").toString(), new String());
                                    }
                                }
                                break;
                            case "int":
                                try{
                                    Any a = record.get(field.get("fieldName").toString());
                                    a.mustBeValid();
                                    map.put(field.get("fieldName").toString(), a.toInt());
                                } catch (Exception e){
                                    try {
                                        Any a = tempEvent.get(field.get("fieldName").toString());
                                        a.mustBeValid();
                                        map.put(field.get("fieldName").toString(), a.toInt());
                                    } catch (Exception ex){
                                        map.put(field.get("fieldName").toString(), 0);
                                    }
                                }
                                break;
                            case "double":
                                try{
                                    Any a = record.get(field.get("fieldName").toString());
                                    a.mustBeValid();
                                    map.put(field.get("fieldName").toString(), a.toDouble());
                                } catch (Exception e){
                                    try {
                                        Any a = tempEvent.get(field.get("fieldName").toString());
                                        a.mustBeValid();
                                        map.put(field.get("fieldName").toString(), a.toDouble());
                                    } catch (Exception ex){
                                        map.put(field.get("fieldName").toString(), 0.0);
                                    }
                                }
                                break;
                            default:
                                try{
                                    Any a = record.get(field.get("fieldName").toString());
                                    a.mustBeValid();
                                    map.put(field.get("fieldName").toString(), a.toInt());
                                } catch (Exception e){
                                    try {
                                        Any a = tempEvent.get(field.get("fieldName").toString());
                                        a.mustBeValid();
                                        map.put(field.get("fieldName").toString(), a.toInt());
                                    } catch (Exception ex){
                                        map.put(field.get("fieldName").toString(), 0);
                                    }
                                }
                                break;
                        }
                    }
                    cepRT.sendEvent(map, "performanceEvent");
                }
                totalConsumedMessages = totalConsumedMessages + tempEvent.get("telemetryDataList").asList().size();
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public long getTotalConsumedMessages() {
        return totalConsumedMessages;
    }

    public void truncateConsumedMessages() {
        totalConsumedMessages = 0;
    }
}
