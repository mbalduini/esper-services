package com.fluxedo.es.throughputTest.MQTT.noDataTransformation;

import com.fluxedo.es.throughputTest.PerformanceProducer;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Created by Marco Balduini on 20/03/2018 as part of project brembo_poc.
 */
public class MQTTNoDataTransformation {

    public static void main(String[] args) {

        try {

            int interval = 5;

            String configFilePath;

            if (args.length > 0) {
                configFilePath = args[0];
            } else {
                configFilePath = "../config/performanceTest/JDBC.json";
            }

            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get(configFilePath))));
            Any connectionConfig =config.get("mqttConnectionInfo");

            //connection information
            String host = connectionConfig.get("host").toString();
            int port = connectionConfig.get("port").toInt();
            String userName = connectionConfig.get("username").toString();
            String password = connectionConfig.get("password").toString();
            String topicID = connectionConfig.get("topic").toString();

            //CDR path
            String CDRSourceFilePath = config.get("dataPath").toString();

            long producerSleep = config.get("producerSleep").toLong();

            org.fusesource.mqtt.client.MQTT mqtt = new org.fusesource.mqtt.client.MQTT();
            try {
                mqtt.setHost(host, port);
                mqtt.setUserName(userName);
                mqtt.setPassword(password);

                PerformanceProducer prod = new PerformanceProducer(mqtt, topicID, producerSleep, CDRSourceFilePath);
                MQTTPerformanceConsumer cons = new MQTTPerformanceConsumer(mqtt, topicID);

                new Thread(prod).start();
                new Thread(cons).start();

                while (true) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(interval * 1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("MESSAGE PRODUCED (per second): " + prod.getTotalProducedMessages() / interval + "\nMESSAGE CONSUMED (per second): " + cons.getTotalConsumedMessages() / interval);

                    prod.truncateTotalProducedMessages();
                    cons.truncateTotalConsumedMessages();
                }
            } catch (URISyntaxException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
