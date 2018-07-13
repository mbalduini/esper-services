package com.fluxedo.es.streamManagers;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventType;
import com.fluxedo.es.commons.StreamManager;
import com.fluxedo.es.utils.Utilities;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import org.fusesource.mqtt.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class SimpleStreamManager extends StreamManager {

    private EPServiceProvider cep;
    private String eventName;
    private long sleepInterval;
    private String sourceFilePath;
    private Thread strThread;

    private Any config;

    private static Logger logger = LoggerFactory.getLogger(SimpleStreamManager.class);

    public SimpleStreamManager() {
    }

    public void initialize(EPServiceProvider cep, String configAsStr) {

        config = JsonIterator.deserialize(configAsStr);

        this.cep = cep;
        this.eventName = config.get("eventName").toString();
        this.sleepInterval = config.get("sleepInterval").toLong();
        this.sourceFilePath = config.get("sourceFilePath").toString();

        cep.getEPAdministrator().createEPL(Utilities.createSchemaEPL(configAsStr));

    }

    public boolean createStream() {

        EPRuntime cepRT = cep.getEPRuntime();

        Runnable pushData = () -> {

            while (true) {
                try {

                    List<HashMap> events = prepareData(sourceFilePath);

                    for (int i = 0 ; i < events.size() ; i++) {
                        String payload = JsonStream.serialize(events.get(i));

                        Any tempEvent = JsonIterator.deserialize(payload);

                        HashMap<String, Object> map;

                        for (Any record : tempEvent.get("telemetryDataList").asList()) {
                            map = new HashMap<>();

                            for (Any field : config.get("dataSchema").asList()) {

                                switch (field.get("fieldType").toString().toLowerCase()) {
                                    case "string":
                                        try {
                                            Any a = record.get(field.get("fieldName").toString());
                                            a.mustBeValid();
                                            map.put(field.get("fieldName").toString(), a.toString());
                                        } catch (Exception e) {
                                            try {
                                                Any a = tempEvent.get(field.get("fieldName").toString());
                                                a.mustBeValid();
                                                map.put(field.get("fieldName").toString(), a.toString());
                                            } catch (Exception ex) {
                                                map.put(field.get("fieldName").toString(), new String());
                                            }
                                        }
                                        break;
                                    case "int":
                                        try {
                                            Any a = record.get(field.get("fieldName").toString());
                                            a.mustBeValid();
                                            map.put(field.get("fieldName").toString(), a.toInt());
                                        } catch (Exception e) {
                                            try {
                                                Any a = tempEvent.get(field.get("fieldName").toString());
                                                a.mustBeValid();
                                                map.put(field.get("fieldName").toString(), a.toInt());
                                            } catch (Exception ex) {
                                                map.put(field.get("fieldName").toString(), 0);
                                            }
                                        }
                                        break;
                                    case "double":
                                        try {
                                            Any a = record.get(field.get("fieldName").toString());
                                            a.mustBeValid();
                                            map.put(field.get("fieldName").toString(), a.toDouble());
                                        } catch (Exception e) {
                                            try {
                                                Any a = tempEvent.get(field.get("fieldName").toString());
                                                a.mustBeValid();
                                                map.put(field.get("fieldName").toString(), a.toDouble());
                                            } catch (Exception ex) {
                                                map.put(field.get("fieldName").toString(), 0.0);
                                            }
                                        }
                                        break;
                                    default:
                                        try {
                                            Any a = record.get(field.get("fieldName").toString());
                                            a.mustBeValid();
                                            map.put(field.get("fieldName").toString(), a.toInt());
                                        } catch (Exception e) {
                                            try {
                                                Any a = tempEvent.get(field.get("fieldName").toString());
                                                a.mustBeValid();
                                                map.put(field.get("fieldName").toString(), a.toInt());
                                            } catch (Exception ex) {
                                                map.put(field.get("fieldName").toString(), 0);
                                            }
                                        }
                                        break;
                                }

                            }
                            cepRT.sendEvent(map, config.get("eventName").toString());
                            TimeUnit.MILLISECONDS.sleep(sleepInterval);
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        };

        strThread = new Thread(pushData);
        strThread.start();

        return true;

    }

    @Override
    public void stopStream() {
        strThread.interrupt();
    }

    private List<HashMap> prepareData(String sourceFilePath){

        String content = new String();

        try {
            content = new String(Files.readAllBytes(Paths.get(sourceFilePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList events = JsonIterator.deserialize(content).get("events").as(ArrayList.class);

        return events;
    }


    @Override
    public EventType getStream() {
        return cep.getEPAdministrator().getConfiguration().getEventType(eventName);
    }
}