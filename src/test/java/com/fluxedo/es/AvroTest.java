package com.fluxedo.es;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class AvroTest {

    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("/Users/baldo/Documents/Work/git/brembo_poc/resources/restServerUtils/brembo-schema-2.json"));

        List<HashMap> events = prepareData("/Users/baldo/Documents/Work/git/brembo_poc/resources/data/dati-brembo/streaming-data-as-single-json-single-event.json");
        String input = JsonStream.serialize(events.get(0));
        Any tempEvent = JsonIterator.deserialize(input);

        try {

            String inputString = tempEvent.toString();
            InputStream inputStream = new ByteArrayInputStream(inputString.getBytes());
            DataInputStream din = new DataInputStream(inputStream);

            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            GenericRecord datum = reader.read(null, decoder);

            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println();
        }

//        Any valueList = tempEvent.get("telemetryDataList");
//
//        for (Any record : valueList) {
//            try {
//
//                InputStream inputStr = new ByteArrayInputStream(record.toString().getBytes());
//                DataInputStream din = new DataInputStream(inputStr);
//
//                Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
//
//                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
//                GenericRecord datum = reader.read(null, decoder);
//                datum.put("devSN", "abc");
//
////                datum.put("onTime", "abc");
////                datum.put("unixTs", "abc");
//
//                System.out.println();
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }






//        EPServiceProvider cep = EPServiceProviderManager.getProvider("http://www.fluxedo.com/cep_brembo");
//        //Register new event type
//        cep.getEPAdministrator().getConfiguration().addEventType(BremboEvent.class);
//        //Register the variable
//        cep.getEPAdministrator().getConfiguration().addVariable(variableName, BremboSDVar.class, bsdv);
//        //Register the query
//        EPStatement cepStatement = cep.getEPAdministrator().createEPL(eplQuery);
//        //Register the listener
//        cepStatement.addListener(new CEPListenerConsolePrinter());

    }

    private static List<HashMap> prepareData(String sourceFilePath){

        String content = new String();

        try {
            content = new String(Files.readAllBytes(Paths.get(sourceFilePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList events = JsonIterator.deserialize(content).get("events").as(ArrayList.class);

        return events;
    }

}
