package com.fluxedo.es.test;

import com.fluxedo.es.commons.Consumer;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by Marco Balduini on 28/06/2018 as part of project esperservices.
 */
public class ReflectionTest {

    public static void main(String[] args) {

        try {


//        ClassLoader cl = ClassLoader.getSystemClassLoader();
//
//        URL[] urls = ((URLClassLoader)cl).getURLs();
//
//        for(URL url: urls){
//            if(url.getFile().contains("/Users/baldo/Documents/Work/git/esper-services"))
//                System.out.println(url.getFile());
////        }
//            File root = new File("");
//
//            URLClassLoader classLoader = classLoader = URLClassLoader.newInstance(new URL[] { root.toURI().toURL() });
//
//
            Reflections reflections = new Reflections("com.fluxedo.es");
            Set<Class<? extends Consumer>> subTypes = reflections.getSubTypesOf(Consumer.class);

            for (Class<? extends Consumer> c : subTypes){
                System.out.println(c.getName());
                System.out.println(c.getTypeName());
                System.out.println(c.getSimpleName());
                System.out.println(c.getName());
            }


            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
