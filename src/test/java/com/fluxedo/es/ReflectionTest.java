package com.fluxedo.es;

import com.fluxedo.es.commons.Consumer;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

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



//            Reflections reflections = new Reflections("com.fluxedo.es");
//            Set<Class<? extends Consumer>> subTypes = reflections.getSubTypesOf(Consumer.class);
//
//            for (Class<? extends Consumer> c : subTypes){
//                System.out.println(c.getName());
//                System.out.println(c.getTypeName());
//                System.out.println(c.getSimpleName());
//                System.out.println(c.getName());
//            }

//            Collection<URL> cu = ClasspathHelper.forJavaClassPath();
//
//            for(URL u : cu)
//                System.out.println(u.toString());
//
//            Reflections reflections = new Reflections(
//                    new ConfigurationBuilder()
//                            .setUrls(ClasspathHelper.forJavaClassPath()));
//            Set<Class<? extends Consumer>> subTypes = reflections.getSubTypesOf(Consumer.class);
//
//            for (Class<? extends Consumer> c : subTypes){
//                System.out.println(c.getName());
//                System.out.println(c.getTypeName());
//                System.out.println(c.getSimpleName());
//                System.out.println(c.getName());
//            }

            ArrayList<URL> collection = new ArrayList<>();

            List<File> l = getFiles("additional-components");
            for (File f : l){
                if (f.getName().endsWith(".jar")){
                    collection.add(f.toURI().toURL());
                }
            }
            collection.addAll(ClasspathHelper.forPackage("com.fluxedo.es.consumers"));

            Reflections reflections = new Reflections(
                    new ConfigurationBuilder().setUrls(collection));
            Set<Class<? extends Consumer>> subTypes = reflections.getSubTypesOf(Consumer.class);



            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<File> getFiles(String paths) {
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
