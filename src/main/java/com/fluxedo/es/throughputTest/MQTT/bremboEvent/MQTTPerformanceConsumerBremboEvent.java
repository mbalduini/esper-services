package com.fluxedo.es.throughputTest.MQTT.bremboEvent;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.fusesource.mqtt.client.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Created by Marco Balduini on 20/03/2018 as part of project brembo_poc.
 */
public class MQTTPerformanceConsumerBremboEvent implements Runnable{

    private MQTT mqtt;
    private String topicId;
    private long totalConsumedMessages = 0;
    private Any config;

    public MQTTPerformanceConsumerBremboEvent(MQTT mqtt, Any config) {
        this.config = config;
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

        BremboEvent be;
        String devSn;
        String onTime;
        LocalDateTime ldt;
        ZoneOffset zo = ZoneId.systemDefault().getRules().getOffset(Instant.now());

        //Start receiving message and push them in Esper
        while (true){
            try {
                Message message = thConnection.receive();
                String payload = new String(message.getPayload());
                totalConsumedMessages++;
                message.ack();

                Any tempEvent = JsonIterator.deserialize(payload);

                onTime = tempEvent.get("onTime").toString();
                devSn = tempEvent.get("devSn").toString();

                for (Any record : tempEvent.get("telemetryDataList").asList()) {
                    be = new BremboEvent();
                    try {
                        be.setValue(record.get("value").toDouble());
                    } catch (Exception e) {
                        be.setValue(0.0);
                    }
                    be.setVarId(record.get("varId").toInt());
                    be.setDevSn(devSn);
                    be.setOnTime(onTime);

                    try {
                        be.setUnixTs(LocalDateTime.parse(onTime, DateTimeFormatter.ofPattern("MMM dd, yyyy h:mm:ss a")).toInstant(zo).toEpochMilli());
                    } catch (DateTimeParseException e) {
                        be.setUnixTs(LocalDateTime.parse(onTime, DateTimeFormatter.ofPattern("MMM dd, yyyy hh:mm:ss a")).toInstant(zo).toEpochMilli());
                    }
                }
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
