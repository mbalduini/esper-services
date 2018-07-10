package com.fluxedo.es.utils;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;

/**
 * Created by Marco Balduini on 05/07/2018 as part of project esperservices.
 */
public class Utilities {

    public static String createSchemaEPL(String dataConfig){

        Any config =  JsonIterator.deserialize(dataConfig);

        String eplStatement = new String();

        //create schema SecurityEvent as (ipAddress string, userId String, numAttempts int)

        eplStatement = "CREATE SCHEMA " + config.get("eventName").toString() + " AS (";

        for(Any field : config.get("dataSchema").asList()){
            eplStatement = eplStatement + (field.get("fieldName").toString() + " " + field.get("fieldType").toString().toUpperCase() + ",");
        }

        eplStatement = eplStatement.substring(0, eplStatement.length() - 1) + ")";
        return eplStatement;
    }
}
