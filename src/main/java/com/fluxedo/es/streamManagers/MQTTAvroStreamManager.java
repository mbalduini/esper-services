package com.fluxedo.es.streamManagers;

import com.espertech.esper.client.ConfigurationEventTypeAvro;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EventType;
import com.fluxedo.es.commons.StreamManager;
import com.jsoniter.JsonIterator;
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
public class MQTTAvroStreamManager extends StreamManager {

    private EPServiceProvider cep;
    private String topicId;
    private MQTT mqtt;
    private String dataSchema;
    private String eventName;
    private Thread strThread;

    public MQTTAvroStreamManager() {
    }

    public void initialize(EPServiceProvider cep, String configAsStr) {

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

        Schema schema = new Schema.Parser().parse(dataSchema);

        ConfigurationEventTypeAvro bremboEventConf = new ConfigurationEventTypeAvro(schema);
        cep.getEPAdministrator().getConfiguration().addEventTypeAvro(eventName, bremboEventConf);

        LocalDateTime ldt;
        ZoneOffset zo = ZoneId.systemDefault().getRules().getOffset(Instant.now());

        Runnable mqttStr = () -> {
            while (true){
                try {
                    Message message = thConnection.receive();
                    String payload = new String(message.getPayload());
                    message.ack();

                    Any tempEvent = JsonIterator.deserialize(payload);
                    Any valueList = tempEvent.get("telemetryDataList");

                    String devSN = tempEvent.get("devSN").toString();
                    String onTime = tempEvent.get("onTime").toString();
                    long unixTs = 0;

                    try{
                        unixTs = LocalDateTime.parse(onTime, DateTimeFormatter.ofPattern("MMM dd, yyyy h:mm:ss a")).toInstant(zo).toEpochMilli();
                    } catch (DateTimeParseException e) {
                        unixTs = LocalDateTime.parse(onTime, DateTimeFormatter.ofPattern("MMM dd, yyyy hh:mm:ss a")).toInstant(zo).toEpochMilli();
                    }


                    for (Any record : valueList) {
                        try {
                            InputStream inputStr = new ByteArrayInputStream(record.toString().getBytes());
                            DataInputStream din = new DataInputStream(inputStr);
                            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
                            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                            GenericRecord datum = reader.read(null, decoder);
                            datum.put("devSN", devSN);
                            datum.put("onTime", onTime);
                            datum.put("unixTs", unixTs);

                            cepRT.sendEventAvro(datum, eventName);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        };


        strThread = new Thread(mqttStr);
        strThread.start();

        return true;

    }

    @Override
    public void stopStream() {
        strThread.interrupt();
    }

    @Override
    public EventType getStream() {
        return cep.getEPAdministrator().getConfiguration().getEventType(eventName);
    }
}