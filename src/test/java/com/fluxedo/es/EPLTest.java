package com.fluxedo.es;

import com.espertech.esper.client.*;
import com.fluxedo.es.consumers.ConsoleConsumer;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.fusesource.mqtt.client.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * Created by Marco Balduini on 21/06/2018 as part of project esperservices.
 */
public class EPLTest {

    public static void main(String[] args) {

        //Join stream and jdbc
        try {

            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/brembo_poc/resources/restServerUtils/registerMQTTStream.json"))));

            String log4jConf = "/Users/baldo/Documents/Work/git/esper-services/src/main/resources/log4j.properties";
            if(log4jConf != null || !log4jConf.isEmpty())
                System.setProperty("log4j.configurationFile",log4jConf);

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

            Configuration engineConfig = new Configuration();

            ConfigurationDBRef configDB = new ConfigurationDBRef();
            configDB.setDriverManagerConnection(
                    "com.mysql.cj.jdbc.Driver",
                    "jdbc:mysql://localhost:3306/test?serverTimezone=UTC",
                    "root",
                    "qwerty");

            ConfigurationDBRef configDB2 = new ConfigurationDBRef();
            configDB2.setDriverManagerConnection(
                    "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "jdbc:sqlserver://0.0.0.0:1433;DatabaseName=test",
                    "SA",
                    "SqLsEr4TeStInG!");

            engineConfig.addDatabaseReference("MysqlConnection", configDB);
            engineConfig.addDatabaseReference("SQLServerConnection", configDB2);

            EPServiceProvider cep = EPServiceProviderManager.getProvider("http://www.fluxedo.com/cep_brembo", engineConfig);

            String createQuery = "CREATE SCHEMA streamEvent AS (varId INT,value DOUBLE,devSn STRING,onTime STRING)";
            cep.getEPAdministrator().createEPL(createQuery);

            //consumer
            createStream(cep ,mqtt, config.get("connectionInfo").get("topic").toString(), config.get("eventName").toString(), config.get("dataSchema").toString());

            String createTableQuery = "create table statiDataInMemoryTable (bb_s_n string primary key, STARTDATETIME string primary key, ENDDATETIME string primary key, ITEMID string)";
            cep.getEPAdministrator().createEPL(createTableQuery);

            String createVariable = "CREATE VARIABLE string lastTimestamp = '1970-01-01 00:00:00'";
            cep.getEPAdministrator().createEPL(createVariable);

            String updateVariable = "ON PATTERN [every timer:interval(1)] SET lastTimestamp = (SELECT COALESCE(MAX(STARTDATETIME), '1970-01-01 00:00:00') FROM statiDataInMemoryTable)";
            cep.getEPAdministrator().createEPL(updateVariable);

            String insertQuery = "INSERT INTO statiDataInMemoryTable " +
                    "SELECT bb_s_n, STARTDATETIME, ENDDATETIME, ITEMID " +
                    "FROM PATTERN [every timer:interval(1)], sql:SQLServerConnection [" +
                    "'SELECT CAST(bb_s_n AS VARCHAR) AS bb_s_n, " +
                    "CAST(STARTDATETIME AS VARCHAR) AS STARTDATETIME, " +
                    "CAST(ENDDATETIME AS VARCHAR) AS ENDDATETIME, " +
                    "CAST(ITEMID AS VARCHAR) AS ITEMID " +
                    "FROM staticData " +
                    "WHERE convert(varchar,STARTDATETIME,25) > convert(varchar,${lastTimestamp},25)'" +
                    "]";
            cep.getEPAdministrator().createEPL(insertQuery);

            String deleteFromTables = "ON PATTERN [every timer:interval(5)] " +
                    "SELECT and DELETE * FROM statiDataInMemoryTable " +
                    "WHERE cast(STARTDATETIME, long, dateformat: 'yyyy-MM-dd HH:dd:ss') < cast('2018-03-26 02:32:49', long, dateformat: 'yyyy-MM-dd HH:dd:ss')";
            cep.getEPAdministrator().createEPL(deleteFromTables);

            String eplQuery = "SELECT varId, value, ITEMID, onTime, STARTDATETIME, ENDDATETIME " +
                    "FROM streamEvent.win:time_batch(1 sec) AS s JOIN statiDataInMemoryTable AS t on s.devSn = t.bb_s_n";
            EPStatement cepStatement = cep.getEPAdministrator().createEPL(eplQuery);
//            cepStatement.addListener(new ConsoleConsumer());

            String test = "SELECT STARTDATETIME FROM statiDataInMemoryTable " +
                    "output snapshot every 4 sec " +
                    "order by cast(STARTDATETIME, long, dateformat: 'yyyy-MM-dd HH:dd:ss') asc";
            cep.getEPAdministrator().createEPL(test).addListener(new ConsoleConsumer());

//            JDBCPerformanceConsumer jdbcc = new JDBCPerformanceConsumer();
//            jdbcc.initialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/esper-services/config/JDBC_Cons_config.json"))));
//            cepStatement.addListener(jdbcc);



        } catch (Exception e) {
            e.printStackTrace();
        }

        //Test with table from jdbc
//        try {
//
//            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/brembo_poc/resources/restServerUtils/registerMQTTStream.json"))));
//
//            JDBC mqtt = new JDBC();
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
//            cepStatement.addListener(new JDBCPerformanceConsoleConsumer());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        //Test with table from stream
//        try {
//
//            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/brembo_poc/resources/restServerUtils/registerMQTTStream.json"))));
//
//            JDBC mqtt = new JDBC();
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
//            cepStatement.addListener(new JDBCPerformanceConsoleConsumer());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        //simple epl example
//        try {
//
//            Any config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/esper-services/config/MQTT_custom_schema_config.json"))));
//
//            String log4jConf = "/Users/baldo/Documents/Work/git/esper-services/src/main/resources/log4j.properties";
//            if(log4jConf != null || !log4jConf.isEmpty())
//                System.setProperty("log4j.configurationFile",log4jConf);
//
//            JDBC mqtt = new JDBC();
//
//            mqtt.setHost(config.get("connectionInfo").get("host").toString(), config.get("connectionInfo").get("port").toInt());
//            mqtt.setUserName(config.get("connectionInfo").get("username").toString());
//            mqtt.setPassword(config.get("connectionInfo").get("password").toString());
//
//            String createQuery = "CREATE SCHEMA testEvent AS (varId INT,value DOUBLE,devSn STRING,onTime STRING)";
//            String eplQuery = "select * from testEvent.win:time_batch(5 sec)";
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
//            cep.getEPAdministrator().createEPL(createQuery);
//
//            //consumer
//            createStream(cep, mqtt, config.get("connectionInfo").get("topic").toString(), config.get("eventName").toString(), config.get("dataSchema").toString());
//
//            EPStatement cepStatement = cep.getEPAdministrator().createEPL(eplQuery);
//            JDBCPerformanceConsumer jdbcc = new JDBCPerformanceConsumer();
//            jdbcc.initialize(new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/esper-services/config/JDBC_Cons_config.json"))));
//            cepStatement.addListener(jdbcc);
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

        Runnable mqttStr = () -> {

            while (true) {
                try {
                    Message message = thConnection.receive();
                    String payload = new String(message.getPayload());
                    message.ack();

                    String schema = new String(Files.readAllBytes(Paths.get("/Users/baldo/Documents/Work/git/esper-services/config/MQTT_custom_schema_config.json")));
                    Any config = JsonIterator.deserialize(schema);
                    Any tempEvent = JsonIterator.deserialize(payload);
                    Any sc = JsonIterator.deserialize(schema);

                    HashMap<String, Object> map;

                    for (Any record : tempEvent.get("telemetryDataList").asList()) {
                        map = new HashMap<>();

                        for (Any field : sc.get("dataSchema").asList()) {

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

        new Thread(mqttStr).start();

        return true;

    }
}

