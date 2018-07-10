package com.fluxedo.es;

import com.fluxedo.es.descriptors.StreamDescriptor;
import com.fluxedo.es.utils.Utilities;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class JsonIterTest {

    public static void main(String[] args) {

        String sourceFilePath = "/Users/baldo/Documents/Work/git/esper-services/config/MQTT_custom_schema_config.json";

        Any config = null;
        try {
            config = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get(sourceFilePath))));
            System.out.println(Utilities.createSchemaEPL(new String(Files.readAllBytes(Paths.get(sourceFilePath)))));
        } catch (IOException e) {
            e.printStackTrace();
        }

//        Any dataConfig = config.get("jdbcConfig").get("dataSchema");
//
//        String query = "INSERT INTO " + config.get("tableName").toString() + "(";
//
//        for(Any field : dataConfig.asList()){
//            query = query + field.get("fieldName").toString() + ",";
//        }
//
//        query = query.substring(0, query.length() - 1) + ") VALUES (";
//
//        for(int i = 0 ; i < dataConfig.asList().size() ; i++){
//            query = query + "?,";
//        }
//
//        query = query.substring(0, query.length() - 1) + ")";
//
//        System.out.println(query);

//        if (s.isEmpty())
//            System.out.println();
//        else
//            System.out.println();

//        String eventName = config.get("eventName").toString();
//
//        System.out.println();

//        StreamDescriptor sd = new StreamDescriptor();
//
//        sd.addStream(new StreamDescriptor("abc", "def"));
//        sd.addStream(new StreamDescriptor("ghi", "lmn"));
//
//        System.out.println(JsonStream.serialize(sd));

    }

}
