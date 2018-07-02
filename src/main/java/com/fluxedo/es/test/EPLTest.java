package com.fluxedo.es.test;

import com.espertech.esper.client.*;
import com.espertech.esperio.db.EsperIODBAdapter;
import com.espertech.esperio.db.config.ConfigurationDBAdapter;
import com.fluxedo.es.commons.BremboEvent;
import com.fluxedo.es.consumers.ConsoleConsumer;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import org.fusesource.mqtt.client.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by Marco Balduini on 21/06/2018 as part of project esperservices.
 */
public class EPLTest {

    public static void main(String[] args) {

        //Join stream and jdbc
        try {

            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/brembo_poc/resources/restServerUtils/registerMQTTStream.json"))));

            MQTT mqtt = new MQTT();

            mqtt.setHost(config.get("connectionInfo").get("host").toString(), config.get("connectionInfo").get("port").toInt());
            mqtt.setUserName(config.get("connectionInfo").get("username").toString());
            mqtt.setPassword(config.get("connectionInfo").get("password").toString());

            //producer
            new Thread(new BremboEventProducer(
                    mqtt,
                    "brembo_topic",
                    1000,
                    "/Users/baldo/Documents/Work/git/brembo_poc/resources/data/dati-brembo/streaming-data-as-single-json.json")
            ).start();


//            String eplQuery2 = "select varId from testEvent.win:time_batch(3 sec)";
//            EPServiceProvider cep1 = EPServiceProviderManager.getProvider("http://www.fluxedo.com/cep_brembo");
//            cep1.getEPAdministrator().createEPL(eplQuery2).addListener(new ConsoleConsumer());
//
//            TimeUnit.MILLISECONDS.sleep(15000);

            //JDBC Adapter configuratiion
            ConfigurationDBRef configDB = new ConfigurationDBRef();
            configDB.setDriverManagerConnection(
                    "com.mysql.cj.jdbc.Driver",
//                    "jdbc:mysql://localhost:3306/test?serverTimezone=UTC",
                    "jdbc:mysql://localhost:3306/?serverTimezone=UTC",
                    "root",
                    "qwerty");

            Configuration engineConfig = new Configuration();
            engineConfig.addDatabaseReference("testConnection", configDB);

            EPServiceProvider cep = EPServiceProviderManager.getProvider("http://www.fluxedo.com/cep_brembo", engineConfig);

            //consumer
            createStream(cep ,mqtt, config.get("connectionInfo").get("topic").toString(), config.get("eventName").toString(), config.get("dataSchema").toString());

            String createQuery = "create table testTable (bb_s_n string primary key, STARTDATETIME string primary key, ENDDATETIME string primary key, ITEMID string)";
            cep.getEPAdministrator().createEPL(createQuery);

            String insertQuery = "insert into testTable select bb_s_n, STARTDATETIME, ENDDATETIME, ITEMID " +
                    "from pattern[timer:interval(5)], sql:testConnection [" +
                    "'select " +
                    "CAST(bb_s_n AS CHAR) AS bb_s_n, " +
                    "CAST(STARTDATETIME AS CHAR) AS STARTDATETIME, " +
                    "CAST(ENDDATETIME AS CHAR) AS ENDDATETIME, " +
                    "CAST(ITEMID AS CHAR) AS ITEMID " +
                    "from test.testTable'" +
                    "]";
            cep.getEPAdministrator().createEPL(insertQuery);

//            String eplQuery = "select * from testEvent.win:time_batch(5 sec)";

            String eplQuery = "select varId, value, ITEMID, onTime, STARTDATETIME, ENDDATETIME " +
                    "from testEvent.win:time_batch(10 sec) as s join testTable as t on s.devSn = t.bb_s_n " +
                    "WHERE unixTs >= cast(STARTDATETIME, long, dateformat: 'yyyy-MM-dd HH:mm:ss') AND unixTs <= cast(ENDDATETIME, long, dateformat: 'yyyy-MM-dd HH:mm:ss')";
            EPStatement cepStatement = cep.getEPAdministrator().createEPL(eplQuery);
            cepStatement.addListener(new ConsoleConsumer());

        } catch (Exception e) {
            e.printStackTrace();
        }


        //Test with table from jdbc
//        try {
//
//            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/brembo_poc/resources/restServerUtils/registerMQTTStream.json"))));
//
//            MQTT mqtt = new MQTT();
//
//            mqtt.setHost(config.get("connectionInfo").get("host").toString(), config.get("connectionInfo").get("port").toInt());
//            mqtt.setUserName(config.get("connectionInfo").get("username").toString());
//            mqtt.setPassword(config.get("connectionInfo").get("password").toString());
//
//            //JDBC Adapter configuratiion
//            ConfigurationDBRef configDB = new ConfigurationDBRef();
//            configDB.setDriverManagerConnection(
//                    "com.mysql.cj.jdbc.Driver",
//                    "jdbc:mysql://localhost:3306/test-db?serverTimezone=UTC",
//                    "root",
//                    "qwerty");
//
//            Configuration engineConfig = new Configuration();
//            engineConfig.addDatabaseReference("testConnection", configDB);
//
//            EPServiceProvider cep = EPServiceProviderManager.getDefaultProvider(engineConfig);
//
//            String eplQuery = "select * from pattern[every timer:interval(10)], sql:testConnection ['select * from testTable']";
//            EPStatement cepStatement = cep.getEPAdministrator().createEPL(eplQuery);
//            cepStatement.addListener(new ConsoleConsumer());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        //Test with table from stream
//        try {
//
//            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/brembo_poc/resources/restServerUtils/registerMQTTStream.json"))));
//
//            MQTT mqtt = new MQTT();
//
//            mqtt.setHost(config.get("connectionInfo").get("host").toString(), config.get("connectionInfo").get("port").toInt());
//            mqtt.setUserName(config.get("connectionInfo").get("username").toString());
//            mqtt.setPassword(config.get("connectionInfo").get("password").toString());
//
//            String createQuery = "create table testTable (varId int primary key, value double primary key, unixTs long primary key)";
//            String insertQuery = "insert into testTable select varId, value, unixTs from testEvent";
////            String eplQuery = "select * from testEvent.win:time_batch(5 sec)";
//
//            EPServiceProvider cep = EPServiceProviderManager.getProvider("http://www.fluxedo.com/cep_brembo");
//
//            //producer
//            new Thread(new BremboEventProducer(
//                    mqtt,
//                    "brembo_topic",
//                    1000,
//                    "/Users/baldo/Documents/Work/git/brembo_poc/resources/data/dati-brembo/streaming-data-as-single-json.json")
//            ).start();
//
//            //consumer
//            createStream(cep,mqtt, config.get("connectionInfo").get("topic").toString(), config.get("eventName").toString(), config.get("dataSchema").toString());
//
//            cep.getEPAdministrator().createEPL(createQuery);
//            cep.getEPAdministrator().createEPL(insertQuery);
//
////            TimeUnit.MILLISECONDS.sleep(20000);
//
//            String eplQuery = "select * from testTable output snapshot every 5 sec";
//            EPStatement cepStatement = cep.getEPAdministrator().createEPL(eplQuery);
//            cepStatement.addListener(new ConsoleConsumer());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }

    public static boolean createStream(EPServiceProvider cep, MQTT mqtt, String topicId, String eventName, String dataSchema) {

        EPRuntime cepRT = cep.getEPRuntime();
        BlockingConnection thConnection = mqtt.blockingConnection();
        try {
            thConnection.connect();
            Topic[] topics = {new Topic(topicId, QoS.AT_LEAST_ONCE)};
            thConnection.subscribe(topics);
        } catch (Exception e) {
            e.printStackTrace();
        }

        cep.getEPAdministrator().getConfiguration().addEventType(eventName, BremboEvent.class);

//        Schema schema = new Schema.Parser().parse(dataSchema);
        //        ConfigurationEventTypeAvro bremboEventConf = new ConfigurationEventTypeAvro(schema);
//        cep.getEPAdministrator().getConfiguration().addEventTypeAvro(eventName, bremboEventConf);

        LocalDateTime ldt;
        ZoneOffset zo = ZoneId.systemDefault().getRules().getOffset(Instant.now());

        Runnable mqttStr = () -> {

            BremboEvent b;

            while (true){
                try {
                    Message message = thConnection.receive();
                    String payload = new String(message.getPayload());
                    message.ack();

                    //String input = JsonStream.serialize(payload);
                    Any tempEvent = JsonIterator.deserialize(payload);
                    Any valueList = tempEvent.get("telemetryDataList");

                    for (Any record : valueList) {
                        try {

                            b = new BremboEvent();

                            b.setDevSn(tempEvent.toString("devSn"));
                            b.setOnTime(tempEvent.toString("onTime"));
                            b.setVarId(record.toInt("varId"));

                            if (record.get("value").valueType() == ValueType.NUMBER)
                                b.setValue(record.toDouble("value"));
                            else
                                b.setValue(0.0);

                            try{
                                b.setUnixTs(LocalDateTime.parse(b.getOnTime(), DateTimeFormatter.ofPattern("MMM dd, yyyy h:mm:ss a")).toInstant(zo).toEpochMilli());
                            } catch (DateTimeParseException e) {
                                b.setUnixTs(LocalDateTime.parse(b.getOnTime(), DateTimeFormatter.ofPattern("MMM dd, yyyy hh:mm:ss a")).toInstant(zo).toEpochMilli());
                            }
                            cepRT.sendEvent(b);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }

            }
        };

        new Thread(mqttStr).start();

        return true;

    }
}
