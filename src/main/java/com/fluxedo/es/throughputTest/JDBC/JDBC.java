package com.fluxedo.es.throughputTest.JDBC;

import com.espertech.esper.client.*;
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
public class JDBC {

    public static void main(String[] args) {

        try {

            int interval = 5;

            String configFilePath;

            if (args.length > 0) {
                configFilePath = args[0];
            } else {
                configFilePath = "../config/JDBC.json";
            }

            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get(configFilePath))));
            Any mqttConnectionConfig =config.get("mqttConnectionInfo");
            Any jdbcConnectionConfig =config.get("jdbcConnectionInfo");

            //connection information
            String host = mqttConnectionConfig.get("host").toString();
            int port = mqttConnectionConfig.get("port").toInt();
            String userName = mqttConnectionConfig.get("username").toString();
            String password = mqttConnectionConfig.get("password").toString();
            String topicID = mqttConnectionConfig.get("topic").toString();

            //CDR path
            String CDRSourceFilePath = config.get("dataPath").toString();

            long producerSleep = config.get("producerSleep").toLong();

            org.fusesource.mqtt.client.MQTT mqtt = new org.fusesource.mqtt.client.MQTT();
            try {
                mqtt.setHost(host, port);
                mqtt.setUserName(userName);
                mqtt.setPassword(password);

                ConfigurationDBRef configDB = new ConfigurationDBRef();
                configDB.setDriverManagerConnection(
                        jdbcConnectionConfig.get("driver").toString(),
                        jdbcConnectionConfig.get("jdbcConnectionString").toString(),
                        jdbcConnectionConfig.get("username").toString(),
                        jdbcConnectionConfig.get("password").toString());

                Configuration engineConfig = new Configuration();
                engineConfig.addDatabaseReference("testConnection", configDB);

                EPServiceProvider cep = EPServiceProviderManager.getProvider("http://www.fluxedo.com/cep_brembo", engineConfig);

                String createQuery = "CREATE SCHEMA performanceEvent AS (varId INT,value DOUBLE,devSn STRING,onTime STRING)";
                cep.getEPAdministrator().createEPL(createQuery);

                String eplQuery = "select varId,value,devSn,onTime from performanceEvent.win:time_batch(1 sec)";
                EPStatement cepStatement = cep.getEPAdministrator().createEPL(eplQuery);

                JDBCPerformanceConsoleConsumer cc = new JDBCPerformanceConsoleConsumer();
                cepStatement.addListener(cc);
                JDBCPerformanceConsumer jdbcc = new JDBCPerformanceConsumer();
                jdbcc.initialize(new String(Files.readAllBytes(Paths.get(configFilePath))));
                cepStatement.addListener(jdbcc);

                PerformanceProducer prod = new PerformanceProducer(mqtt, topicID, producerSleep, CDRSourceFilePath);
                JDBCPerformanceDataInjecterWithDataTransformation cons = new JDBCPerformanceDataInjecterWithDataTransformation(cep, mqtt, new String(Files.readAllBytes(Paths.get(configFilePath))));

                new Thread(prod).start();
                new Thread(cons).start();

                while (true) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(interval * 1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    System.out.println("MESSAGE INJECTED IN ESPER (per second): " + cons.getTotalConsumedMessages() / interval + "\nMESSAGE CONSUMED BY CONSOLE CONSUMER (per second): " + cc.getTotalConsumedMessages() / interval + "\nMESSAGE CONSUMED BY JDBC CONSUMER (per second) : " + jdbcc.getTotalConsumedMessages() / interval);
                    cons.truncateConsumedMessages();
                    cc.truncateConsumedMessages();
                    jdbcc.truncateConsumedMessages();
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
