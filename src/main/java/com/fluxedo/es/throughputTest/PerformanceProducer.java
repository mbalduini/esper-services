package com.fluxedo.es.throughputTest;

import com.jsoniter.JsonIterator;
import com.jsoniter.output.JsonStream;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Marco Balduini on 20/03/2018 as part of project brembo_poc.
 */
public class PerformanceProducer implements Runnable{

    //togliere trasformazione in CDREvent

    private MQTT thmqtt;
    private String topicId;
    private long sleepInterval;
    private String sourceFilePath;
    private long totalProducedMessages = 0;

    public PerformanceProducer(MQTT thmqtt, String topicId, long sleepInterval, String sourceFilePath) {
        this.thmqtt = thmqtt;
        this.topicId = topicId;
        this.sleepInterval = sleepInterval;
        this.sourceFilePath = sourceFilePath;
    }

    public void run() {

        List<HashMap> events = prepareData(sourceFilePath);

        BlockingConnection thConnection = thmqtt.blockingConnection();
        try {
            thConnection.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(sleepInterval > 0) {
            while (true) {
                for (int i = 0; i < events.size(); i++) {
                    try {
                        String input = JsonStream.serialize(events.get(i));
                        totalProducedMessages++;
                        thConnection.publish(topicId, input.toString().getBytes(), QoS.AT_LEAST_ONCE, false);
                        Thread.sleep(sleepInterval);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            while (true) {

                for (int i = 0; i < events.size(); i++) {
                    try {
                        String input = JsonStream.serialize(events.get(i));
                        totalProducedMessages++;
                        thConnection.publish(topicId, input.toString().getBytes(), QoS.AT_LEAST_ONCE, false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    //Create a collection of String
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

    public long getTotalProducedMessages() {
        return totalProducedMessages;
    }

    public void truncateTotalProducedMessages() {
        totalProducedMessages = 0;
    }
}
