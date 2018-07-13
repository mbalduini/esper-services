package com.fluxedo.es.streamManagers;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventType;
import com.fluxedo.es.commons.StreamManager;
import com.fluxedo.es.utils.Utilities;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.fusesource.mqtt.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class MQTTStreamManager extends StreamManager {

    private EPServiceProvider cep;
    private String topicId;
    private MQTT mqtt;
    private String eventName;
    private Thread strThread;

    private Any config;

    private static Logger logger = LoggerFactory.getLogger(MQTTStreamManager.class);

    public MQTTStreamManager() {
    }

    public void initialize(EPServiceProvider cep, String configAsStr) {

        config = JsonIterator.deserialize(configAsStr);
        Any ci = config.get("connectionInfo");

        this.cep = cep;
        this.topicId = ci.get("topic").toString();
        this.eventName = config.get("eventName").toString();

        mqtt = new MQTT();
        try {
            mqtt.setHost(ci.get("host").toString(), ci.get("port").toInt());
            mqtt.setUserName(ci.get("username").toString());
            mqtt.setPassword(ci.get("password").toString());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error while connecting to JDBC service.", e);
        }

        cep.getEPAdministrator().createEPL(Utilities.createSchemaEPL(configAsStr));

    }

    public boolean createStream() {

        EPRuntime cepRT = cep.getEPRuntime();
        BlockingConnection thConnection = mqtt.blockingConnection();
        try {
            thConnection.connect();
            Topic[] topics = {new Topic(topicId, QoS.AT_LEAST_ONCE)};
            thConnection.subscribe(topics);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Runnable mqttStr = () -> {

            while (true) {
                try {
                    Message message = thConnection.receive();
                    String payload = new String(message.getPayload());
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
                        cepRT.sendEvent(map, config.get("eventName").toString());
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        };

        strThread = new Thread(mqttStr);
        strThread.start();

        return true;

    }

    @Override
    public void stopStream() {
        strThread.interrupt();
    }

    @Override
    public EventType getStream() {
        return cep.getEPAdministrator().getConfiguration().getEventType(eventName);
    }
}