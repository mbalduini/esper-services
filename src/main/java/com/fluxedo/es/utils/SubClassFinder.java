package com.fluxedo.es.utils;

import com.fluxedo.es.commons.Consumer;
import com.fluxedo.es.commons.StreamManager;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.*;

/**
 * Created by Marco Balduini on 28/06/2018 as part of project esperservices.
 */
public class SubClassFinder {

    private static Logger logger = LoggerFactory.getLogger(SubClassFinder.class);

    public static Map<String, Class<? extends Consumer>> findConsumerSubclasses(String packageName){

        HashMap<String, Class<? extends Consumer>> map = new HashMap<String, Class<? extends Consumer>>();

        try {
            ArrayList<URL> collection = new ArrayList<>();

            List<File> l = getFiles("additional-components");
            for (File f : l) {
                if (f.getName().endsWith(".jar")) {
                    collection.add(f.toURI().toURL());
                }
            }
            collection.addAll(ClasspathHelper.forPackage(packageName));

            Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(collection));
            Set<Class<? extends Consumer>> subTypes = reflections.getSubTypesOf(Consumer.class);

            for (Class<? extends Consumer> c : subTypes) {
                map.put(c.getSimpleName(), c);
            }
        } catch (Exception e) {
            logger.error("Error while retrieving sub-classes.", e);
        }

        return map;
    }

    public static Map<String, Class<? extends StreamManager>> findStreamManagerSubclasses(String packageName){

        HashMap<String, Class<? extends StreamManager>> map = new HashMap<>();

        try {
            ArrayList<URL> collection = new ArrayList<>();

            List<File> l = getFiles("additional-components");
            for (File f : l) {
                if (f.getName().endsWith(".jar")) {
                    collection.add(f.toURI().toURL());
                }
            }
            collection.addAll(ClasspathHelper.forPackage(packageName));

            Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(collection));
            Set<Class<? extends StreamManager>> subTypes = reflections.getSubTypesOf(StreamManager.class);

            for(Class<? extends StreamManager> sm : subTypes){
                map.put(sm.getSimpleName(), sm);
            }
        } catch (Exception e) {
            logger.error("Error while retrieving sub-classes.", e);
        }

        return map;
    }

    private static List<File> getFiles(String paths) {
        List<File> filesList = new ArrayList<File>();
        for (final String path : paths.split(File.pathSeparator)) {
            final File file = new File(path);
            if( file.isDirectory()) {
                recurse(filesList, file);
            }
            else {
                filesList.add(file);
            }
        }
        return filesList;
    }

    private static void recurse(List<File> filesList, File f) {
        File list[] = f.listFiles();
        for (File file : list) {
            if (file.isDirectory()) {
                recurse(filesList, file);
            }
            else {
                filesList.add(file);
            }
        }
    }
}
