package com.fluxedo.es;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.fusesource.mqtt.client.MQTT;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Marco Balduini on 02/07/2018 as part of project esperservices.
 */
public class StartPocProducer {

    public static void main(String[] args) {
        try {

            String configFilePath;

            if(args.length > 0){
                configFilePath = args[0];
            } else {
                configFilePath = "./config/MQTT_test_producer.json";
            }

            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get(configFilePath))));

            MQTT mqtt = new MQTT();

            mqtt.setHost(config.get("connectionInfo").get("host").toString(), config.get("connectionInfo").get("port").toInt());
            mqtt.setUserName(config.get("connectionInfo").get("username").toString());
            mqtt.setPassword(config.get("connectionInfo").get("password").toString());

            new Thread(new BremboEventProducer(
                    mqtt,
                    config.get("connectionInfo").get("topic").toString(),
                    config.get("sleepInterval").toLong(),
                    config.get("sourceFilePath").toString())
            ).start();

        } catch (Exception e){
            e.printStackTrace();
        }
    }

}
