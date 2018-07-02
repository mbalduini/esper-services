package com.fluxedo.es.utils;


import com.fluxedo.es.commons.Consumer;
import com.fluxedo.es.commons.StreamManager;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Marco Balduini on 28/06/2018 as part of project esperservices.
 */
public class SubClassFinder {

    public static Map<String, Class<? extends Consumer>> findConsumerSubclasses(){

        HashMap<String, Class<? extends Consumer>> map = new HashMap<String, Class<? extends Consumer>>();

        Reflections reflections = new Reflections("com.fluxedo.es");
        Set<Class<? extends Consumer>> subTypes = reflections.getSubTypesOf(Consumer.class);

        for(Class<? extends Consumer> c : subTypes){
            map.put(c.getSimpleName(), c);
        }

        return map;
    }

    public static Map<String, Class<? extends StreamManager>> findStreamManagerSubclasses(){

        HashMap<String, Class<? extends StreamManager>> map = new HashMap<>();

        Reflections reflections = new Reflections("com.fluxedo.es");
        Set<Class<? extends StreamManager>> subTypes = reflections.getSubTypesOf(StreamManager.class);

        for(Class<? extends StreamManager> sm : subTypes){
            map.put(sm.getSimpleName(), sm);
        }

        return map;
    }
}
