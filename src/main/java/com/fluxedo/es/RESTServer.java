package com.fluxedo.es;

import com.espertech.esper.client.*;
import com.fluxedo.es.commons.Consumer;
import com.fluxedo.es.commons.StandardResponse;
import com.fluxedo.es.commons.StatusResponse;
import com.fluxedo.es.commons.StreamManager;
import com.fluxedo.es.descriptors.*;
import com.fluxedo.es.internalPurposeDescriptor.ConsumerDescriptorIP;
import com.fluxedo.es.internalPurposeDescriptor.QueryDescriptorIP;
import com.fluxedo.es.internalPurposeDescriptor.StreamDescriptorIP;
import com.fluxedo.es.utils.SubClassFinder;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static spark.Spark.*;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class RESTServer {

    private static HashMap<String, QueryDescriptorIP> queryList = new HashMap<>();
    private static HashMap<String, ConsumerDescriptorIP> consumerList = new HashMap<>();
    private static HashMap<String, StreamDescriptorIP> streamList = new HashMap<>();
    private static Logger logger = LoggerFactory.getLogger(RESTServer.class);


    public static void main(String[] args) {

        try {

            String configFilePath;

            if(args.length > 0){
                configFilePath = args[0];
            } else {
                configFilePath = "./config/config.json";
            }

            Any serverConfig = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get(configFilePath)))).get("serverConfig");

            port(serverConfig.get("port").toInt());

            Map<String, Class<? extends Consumer>> cMap = SubClassFinder.findConsumerSubclasses();
            Map<String, Class<? extends StreamManager>> smMap = SubClassFinder.findStreamManagerSubclasses();

            EPServiceProvider cep = EPServiceProviderManager.getProvider(serverConfig.get("cepURI").toString());

            logger.info("Server started on port: {}.\nA new cep instance with URI {} is now available.", serverConfig.get("port").toInt(), serverConfig.get("cepURI").toString());
            System.out.println("Server started on port: " + serverConfig.get("port").toString() + ".\nA new cep instance called " + serverConfig.get("cepURI").toString() + " is now available.");

            //STREAMS

            put("/streams", (request, response) -> {

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    Class<? extends StreamManager> csm = smMap.get(config.get("type").toString());
                    StreamManager sm = csm.newInstance();
                    sm.initialize(cep, config.toString());
                    sm.createStream();

                    streamList.put(sm.getStream().getName(), new StreamDescriptorIP(sm));

                    message = sm.getStream().getName() + " successfully registered";
                    logger.info(message);

//                    EPStatement cepStatement = cep.getEPAdministrator().createEPL("select * from testEvent.win:time_batch(5 sec)");
//                    cepStatement.addListener(new ConsoleConsumer());

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e){
                    logger.error("Error while creating new stream", e);
                    message = "Error while creating new stream. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/streams", (request, response) -> {

                String message;

                try {
                    Collection<StreamDescriptorIP> ets = streamList.values();
                    ArrayList<StreamDescriptor> streams = new ArrayList<>();

                    for(StreamDescriptorIP sd : ets){
                        streams.add(new StreamDescriptor(sd.getSm().getStream().getName(),sd.getSm().getStream().getUnderlyingType().getTypeName()));
                    }

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, streams));
                } catch (Exception e) {
                    logger.error("Error while retrieving streams info", e);
                    message = "Error while retrieving streams info. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/streams", (request, response) -> {

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());
                    cep.getEPAdministrator().getConfiguration().removeEventType(config.get("eventName").toString(), config.get("force").toBoolean());
                    message = "Stream " + config.get("eventName").toString() + " succesfully deleted";
                    logger.info(message);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e){
                    logger.error("Error while deleting stream", e);
                    message = "Error while creating deleting stream. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/streams/:name", (request, response) -> {

                String message;

                try {
                    StreamDescriptorIP sd = streamList.get(request.params(":name"));
                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, new StreamDescriptor(sd.getSm().getStream().getName(), sd.getSm().getStream().getUnderlyingType().getTypeName())));
                } catch (Exception e) {
                    logger.error("Error while retrieving stream info", e);
                    message = "Error while retrieving stream info. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/streams/:name", (request, response) -> {

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());
                    cep.getEPAdministrator().getConfiguration().removeEventType(request.params(":name"), config.get("force").toBoolean());
                    message = "Stream " + request.params(":name") + " succesfully deleted";
                    logger.info(message);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e){
                    logger.error("Error while deleting stream", e);
                    message = "Error while creating deleting stream. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            //EPL

            put("/epl", (request, response) -> {

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    EPStatement cepStatement = cep.getEPAdministrator().createEPL(config.get("eplQuery").toString());
//            cepStatement.addListener(new ConsoleConsumer());
                    queryList.put(cepStatement.getName(), new QueryDescriptorIP(cepStatement));

                    message = "EPL statement " + cepStatement.getName() + " succesfully registered";
                    logger.info(message);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e){
                    logger.error("Error while registering EPL statement", e);
                    message = "Error while registering EPL statement. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/epl", (request, response) -> {

                String message;

                try {
                    Collection<QueryDescriptorIP> qdList = queryList.values();
                    ArrayList<QueryDescriptor> queries = new ArrayList<>();

                    for ( QueryDescriptorIP qd : qdList){

                        HashMap <String, ConsumerDescriptor> cm = new HashMap<>();
                        for (ConsumerDescriptorIP cd : qd.getConsumers().values()){
                            cm.put(cd.getEPLQueryName(), new ConsumerDescriptor(cd.getId(), cd.getEPLQueryName(), cd.getType()));
                        }

                        queries.add(new QueryDescriptor(qd.getEpStatement().getName(), qd.getEpStatement().getText(), qd.getEpStatement().getState().toString(), cm));
                    }

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, queries));
                } catch (Exception e) {
                    logger.error("Error while retrieving EPL statements info", e);
                    message = "Error while retrieving EPL statements info. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            post("/epl", (request, response) -> {

                String message = new String();

                try {

                    Any config = JsonIterator.deserialize(request.body());
                    switch (config.get("action").toString().toLowerCase()) {
                        case "stop": {
                            QueryDescriptorIP qd = queryList.get(config.get("statementName").toString());
                            if (qd.getEpStatement().getState().toString().equals("STARTED")) {
                                qd.getEpStatement().stop();
                                queryList.put(config.get("statementName").toString(), qd);
                                message = "The statement " + config.get("statementName").toString() + " is now " + qd.getEpStatement().getState().toString() + ".";
                            } else {
                                message = "The statement " + config.get("statementName").toString() + " is in " + qd.getEpStatement().getState().toString() + " state, and cannot be stopped.";
                                logger.info(message);
                                return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                            }
                            break;
                        }
                        case "start": {
                            QueryDescriptorIP qd = queryList.get(config.get("statementName").toString());
                            if (qd.getEpStatement().getState().toString().equals("STOPPED")) {
                                qd.getEpStatement().start();
                                queryList.put(config.get("statementName").toString(), qd);
                                message = "The statement " + config.get("statementName").toString() + " is now " + qd.getEpStatement().getState().toString() + ".";
                            } else {
                                message = "The statement " + config.get("statementName").toString() + " is in " + qd.getEpStatement().getState().toString() + " state, and cannot be started.";
                                logger.info(message);
                                return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                            }
                            break;
                        }
                    }

                    logger.info(message);
                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e) {
                    logger.error("Error while modifying EPL statement state", e);
                    message = "Error while modifying EPL statement state. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/epl", (request, response) -> {

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());
                    EPStatement eps = cep.getEPAdministrator().getStatement(config.get("statementName").toString());

                    queryList.remove(eps.getName());
                    eps.destroy();

                    message = eps.getName() + " EPL statement succesfully removed.";
                    logger.info(message);

                    return JsonStream.serialize(StatusResponse.SUCCESS);
                } catch (Exception e) {
                    logger.error("Error while removing EPL statement", e);
                    message = "Error while removing EPL statement. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/epl/:name", (request, response) -> {

                String message;

                try {
                    QueryDescriptorIP qd = queryList.get(request.params(":name"));

                    HashMap <String, ConsumerDescriptor> cm = new HashMap<>();
                    for (ConsumerDescriptorIP cd : qd.getConsumers().values()){
                        cm.put(cd.getEPLQueryName(), new ConsumerDescriptor(cd.getId(), cd.getEPLQueryName(), cd.getType()));
                    }

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS,
                            new QueryDescriptor(qd.getEpStatement().getName(), qd.getEpStatement().getText(), qd.getEpStatement().getState().toString(), cm)));
                } catch (Exception e) {
                    logger.error("Error while retrieving ELP statement info", e);
                    message = "Error while retrieving ELP statement info. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/epl:name", (request, response) -> {

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());
                    EPStatement eps = cep.getEPAdministrator().getStatement(request.params(":name"));

                    queryList.remove(eps.getName());
                    eps.destroy();

                    message = eps.getName() + " EPL statement succesfully removed.";
                    logger.info(message);

                    return JsonStream.serialize(StatusResponse.SUCCESS);
                } catch (Exception e) {
                    logger.error("Error while removing EPL statement", e);
                    message = "Error while removing EPL statement. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            //CONSUMER

            put("/consumers", (request, response) -> {

                String message;

                try {
                    Any config = JsonIterator.deserialize(request.body());

                    Class<? extends Consumer> cc = cMap.get(config.get("type").toString());
                    Consumer c = cc.newInstance();
                    c.initialize(config.toString());

                    QueryDescriptorIP qd = queryList.get(config.get("eplQueryName").toString());
                    qd.getEpStatement().addListener(c);
                    ConsumerDescriptorIP cd = new ConsumerDescriptorIP(c, c.getId().toString(), qd.getEpStatement().getName(), config.get("type").toString());
                    qd.addConsumer(cd);
                    queryList.put(config.get("eplQueryName").toString(), qd);
                    consumerList.put(c.getId().toString(), cd);

                    message = "Consumer " + config.get("type").toString() + " succesfully connected to the EPL statement " + config.get("type").toString();
                    logger.info(message);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e) {
                    logger.error("Error while registering consumer", e);
                    message = "Error while registering consumer. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }

            });

            delete("/consumers/:id", (request, response) -> {

                String message;

                try {
                    ConsumerDescriptorIP cs = consumerList.get(request.params(":id"));
                    QueryDescriptorIP qd = queryList.get(cs.getEPLQueryName());
                    qd.getEpStatement().removeListener(cs.getConsumer());
                    queryList.put(qd.getEpStatement().getName(), qd);

                    message = "Consumer " + cs.getId() + " stop listening to the stream created by " + qd.getEpStatement().getName() + " ELS statement.";
                    logger.info(message);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e) {
                    logger.error("Error while deleting consumer.", e);
                    message = "Error while deleting consumer. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
