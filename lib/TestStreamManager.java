package com.fluxedo.es.streamManagers;

import com.espertech.esper.client.ConfigurationEventTypeAvro;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.fluxedo.es.commons.BremboEvent;
import com.fluxedo.es.commons.StreamManager;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.fusesource.mqtt.client.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class TestStreamManager extends StreamManager {

    private EPServiceProvider cep;
    private String topicId;
    private MQTT mqtt;
    private String eventName;

    public TestStreamManager(EPServiceProvider cep, String configAsStr) {

        //String eventName, String topicId, String host, int port, String username, String pwd)

//        new MQTTStreamManager(
//                cep,
//                config.get("eventName").toString(),
//                config.get("topic").toString(),
//                config.get("host").toString(),
//                Integer.parseInt(config.get("port").toString()),
//                config.get("username").toString(),
//                config.get("password").toString()).createStream();
//        break;

        Any config = JsonIterator.deserialize(configAsStr);

        this.cep = cep;
        this.topicId = config.get("topic").toString();
        this.eventName = config.get("eventName").toString();

        MQTT mqtt = new MQTT();
        try {
            mqtt.setHost(config.get("host").toString(), config.get("port").toInt());
            mqtt.setUserName(config.get("username").toString());
            mqtt.setPassword(config.get("password").toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean createStream() {

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

        ZoneOffset zo = ZoneId.systemDefault().getRules().getOffset(Instant.now());

        Runnable mqttStr = () -> {

            BremboEvent b;

            while (true) {
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

                            try {
                                b.setUnixTs(LocalDateTime.parse(b.getOnTime(), DateTimeFormatter.ofPattern("MMM dd, yyyy h:mm:ss a")).toInstant(zo).toEpochMilli());
                            } catch (DateTimeParseException e) {
                                b.setUnixTs(LocalDateTime.parse(b.getOnTime(), DateTimeFormatter.ofPattern("MMM dd, yyyy hh:mm:ss a")).toInstant(zo).toEpochMilli());
                            }
                            cepRT.sendEvent(b);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        };

        new Thread(mqttStr).start();

        return true;

    }
}