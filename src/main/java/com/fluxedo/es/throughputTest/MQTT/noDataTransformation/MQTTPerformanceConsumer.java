package com.fluxedo.es.throughputTest.MQTT.noDataTransformation;

import org.fusesource.mqtt.client.*;
import org.fusesource.mqtt.client.MQTT;

/**
 * Created by Marco Balduini on 20/03/2018 as part of project brembo_poc.
 */
public class MQTTPerformanceConsumer implements Runnable{

    private MQTT mqtt;
    private String topicId;
    private long totalConsumedMessages = 0;

    public MQTTPerformanceConsumer(MQTT mqtt, String topicId) {
        this.mqtt = mqtt;
        this.topicId = topicId;
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

        //Start receiving message and push them in Esper
        while (true){
            try {
                Message message = thConnection.receive();
                String payload = new String(message.getPayload());
                totalConsumedMessages++;
                message.ack();
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public long getTotalConsumedMessages() {
        return totalConsumedMessages;
    }

    public void truncateTotalConsumedMessages() {
        totalConsumedMessages = 0;
    }
}
