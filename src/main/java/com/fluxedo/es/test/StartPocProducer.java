package com.fluxedo.es.test;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.fusesource.mqtt.client.MQTT;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Marco Balduini on 02/07/2018 as part of project esperservices.
 */
public class StartPocProducer {

    private static String jsonConfigPath = "/Users/baldo/Documents/Work/git/esper-services/config/MQTT_config.json";

    public static void main(String[] args) {
        try {

            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get(jsonConfigPath))));

            MQTT mqtt = new MQTT();

            mqtt.setHost(config.get("connectionInfo").get("host").toString(), config.get("connectionInfo").get("port").toInt());
            mqtt.setUserName(config.get("connectionInfo").get("username").toString());
            mqtt.setPassword(config.get("connectionInfo").get("password").toString());

            new Thread(new BremboEventProducer(
                    mqtt,
                    "brembo_topic",
                    1000,
                    "/Users/baldo/Documents/Work/git/brembo_poc/resources/data/dati-brembo/streaming-data-as-single-json.json")
            ).start();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

}
