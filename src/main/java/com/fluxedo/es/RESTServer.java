package com.fluxedo.es;

import com.espertech.esper.client.*;
import com.fluxedo.es.exceptions.CEPNotFoundException;
import com.fluxedo.es.exceptions.DuplicationException;
import com.fluxedo.es.commons.Consumer;
import com.fluxedo.es.commons.StandardResponse;
import com.fluxedo.es.commons.StatusResponse;
import com.fluxedo.es.commons.StreamManager;
import com.fluxedo.es.descriptors.*;
import com.fluxedo.es.internalPurposeDescriptor.CEPDescriptorIP;
import com.fluxedo.es.internalPurposeDescriptor.ConsumerDescriptorIP;
import com.fluxedo.es.internalPurposeDescriptor.QueryDescriptorIP;
import com.fluxedo.es.internalPurposeDescriptor.StreamDescriptorIP;
import com.fluxedo.es.utils.SubClassFinder;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.EncodingMode;
import com.jsoniter.output.JsonStream;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.server.ServerContainer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static spark.Spark.*;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class RESTServer {

    private static HashMap<String, CEPDescriptorIP> cepList = new HashMap<>();
    private static HashMap<String, QueryDescriptorIP> queryList = new HashMap<>();
    private static HashMap<String, ConsumerDescriptorIP> consumerList = new HashMap<>();
    private static HashMap<String, StreamDescriptorIP> streamList = new HashMap<>();
    private static Logger logger = LoggerFactory.getLogger(RESTServer.class);

    public static ServerContainer wscontainer;

    public static void main(String[] args) {

        try {

            //System.setProperty("log4j.configurationFile","/Users/baldo/Documents/Work/git/esper-services/config/logConfig/log4j2.properties");

            JsonStream.setMode(EncodingMode.DYNAMIC_MODE);

            String configFilePath;

            if(args.length > 0){
                configFilePath = args[0];
            } else {
                configFilePath = "./config/config.json";
            }

            Any serverConfig = JsonIterator.deserialize(new String(Files.readAllBytes(Paths.get(configFilePath))));

            String log4jConf = serverConfig.get("logConfigurator").toString();
            if(log4jConf != null || !log4jConf.isEmpty())
                System.setProperty("log4j.configurationFile",log4jConf);

            if(serverConfig.get("port").toString() != "")
                port(serverConfig.get("port").toInt());
            else
                port(3456);

            Map<String, Class<? extends Consumer>> cMap = SubClassFinder.findConsumerSubclasses("com.fluxedo.es.consumers");
            Map<String, Class<? extends StreamManager>> smMap = SubClassFinder.findStreamManagerSubclasses("com.fluxedo.es.streamManagers");

            int wsPort;

            if(serverConfig.get("wsServerPort").toString() != "")
                wsPort = serverConfig.get("wsServerPort").toInt();
            else
                wsPort = 3457;

            try {
                Server server = new Server();
                ServerConnector connector = new ServerConnector(server);
                connector.setPort(wsPort);
                server.addConnector(connector);

                ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
                context.setContextPath("/");
                server.setHandler(context);
                wscontainer = WebSocketServerContainerInitializer.configureContext(context);
                server.start();
            } catch (Exception e){
                logger.error("Error while starting Websocket Server", e);
            }

            logger.info("Rest Server started on port: {}. Websocket Server started on port: {}", serverConfig.get("port").toInt(), wsPort);

            //CEP

            post("/cep", (request, response) -> {
                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    if(cepList.containsKey(config.get("cepURI").toString())){
                        throw new DuplicationException("A cep instance with the same URI is already instantiated.");
                    }

                    EPServiceProvider cep;
                    CEPDescriptorIP cdi = new CEPDescriptorIP();

                    cdi.setURI(config.get("cepURI").toString());

                    Any jdbcConncections = config.get("JDBCConnections");
                    if(jdbcConncections.size() != 0) {
                        Configuration engineConfig = new Configuration();
                        for(Any jdbcConncection : jdbcConncections.asList()) {

                            ConfigurationDBRef configDB = new ConfigurationDBRef();
                            configDB.setDriverManagerConnection(
                                    jdbcConncection.get("driver").toString(),
                                    jdbcConncection.get("url").toString(),
                                    jdbcConncection.get("username").toString(),
                                    jdbcConncection.get("password").toString());
                            engineConfig.addDatabaseReference(jdbcConncection.get("connectionName").toString(), configDB);

                            cdi.addJDBCConnection(
                                    new JDBCConnectionDescriptor(
                                            jdbcConncection.get("connectionName").toString(),
                                            jdbcConncection.get("driver").toString(),
                                            jdbcConncection.get("url").toString(),
                                            jdbcConncection.get("username").toString(),
                                            jdbcConncection.get("password").toString()));
                        }
                        cep = EPServiceProviderManager.getProvider(config.get("cepURI").toString(),engineConfig);
                    } else {
                        cep = EPServiceProviderManager.getProvider(config.get("cepURI").toString());
                    }

                    cdi.setCep(cep);
                    cepList.put(config.get("cepURI").toString(), cdi);

                    message = "New cep instance, with URI: " + config.get("cepURI").toString() + ", successfully created.";
                    logger.info(message);

//                    TimeUnit.MILLISECONDS.sleep(2000);

                    response.type("application/json");
                    response.status(HttpStatus.CREATED_201);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message, new CEPDescriptor(cdi.getURI(), cdi.getJDBCConnections())));
                } catch (DuplicationException e){
                    logger.error("A cep instance with the same URI is already instantiated.", e);
                    message = "A cep instance with the same URI is already instantiated.";

                    response.type("application/json");
                    response.status(HttpStatus.CONFLICT_409);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                } catch (Exception e){
                    e.printStackTrace();

                    logger.error("Error while instantiating new cep instance", e);
                    message = "Error while instantiating new cep instance. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/cep", (request, response) -> {

                String message;

                try {
                    Collection<CEPDescriptorIP> ets = cepList.values();
                    ArrayList<CEPDescriptor> ceps = new ArrayList<>();

                    for(CEPDescriptorIP cdi : ets){
                        ceps.add(new CEPDescriptor(cdi.getURI(), cdi.getJDBCConnections()));
                    }

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, ceps));
                } catch (Exception e) {
                    logger.error("Error while retrieving cep list", e);
                    message = "Error while retrieving cep list. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/cep", (request, response) -> {

                response.type("application/json");
                response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, "API CEP to remove a CEP. This API currently do nothing, it is not yet ready."));

                //TBC -> before deleting a cep you must destroy all the streammanager that send data into it

//                response.type("application/json");
//
//                String message;
//
//                try {
//                    Any config = JsonIterator.deserialize(request.body());
//
//                    EPServiceProvider cep = cepList.get(config.get("cepURI").toString()).getCep();
//                    cep.destroy();
//                    cepList.remove(config.get("cepURI").toString());
//                    message = "CEP " + config.get("cepURI").toString() + " succesfully deleted";
//                    logger.info(message);
//
//                    response.type("application/json");
//                    response.status(HttpStatus.OK_200);
//
//                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
//                } catch (Exception e){
//                    logger.error("Error while deleting CEP", e);
//                    message = "Error while creating deleting CEP. See log messages for further information.";
//
//                    response.type("application/json");
//                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);
//
//                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
//                }
            });

            //STREAMS

            post("/streams", (request, response) -> {
                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    if (cepList.get(config.get("cepURI").toString()) == null)
                        throw new CEPNotFoundException("A CEP with the specified URi is not yet instantiated.");
                    EPServiceProvider cep = cepList.get(config.get("cepURI").toString()).getCep();

                    if (streamList.containsKey(config.get("eventName").toString()))
                        throw new DuplicationException("A stream with the same name is already registered.");

                    Class<? extends StreamManager> csm = smMap.get(config.get("type").toString());
                    StreamManager sm = csm.newInstance();
                    sm.initialize(cep, request.body());
                    sm.createStream();

                    StreamDescriptorIP sdi = new StreamDescriptorIP(config.get("cepURI").toString(), sm);

                    streamList.put(sm.getStream().getName(), sdi);

                    message = sm.getStream().getName() + " successfully registered";
                    logger.info(message);

//                    EPStatement cepStatement = cep.getEPAdministrator().createEPL("select * from testEvent.win:time_batch(5 sec)");
//                    cepStatement.addListener(new JDBCPerformanceConsoleConsumer());

                    response.type("application/json");
                    response.status(HttpStatus.CREATED_201);

//                    TimeUnit.MILLISECONDS.sleep(2000);

                    StreamDescriptor sd = new StreamDescriptor();
                    sd.setCepURI(sdi.getCepURI());
                    sd.setName(sdi.getSm().getStream().getName());
                    sd.setType(sdi.getSm().getStream().getUnderlyingType().getTypeName());
                    for(String s : sdi.getSm().getStream().getPropertyNames())
                        sd.addField(s, sdi.getSm().getStream().getPropertyType(s).getTypeName());

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message, sd));
                } catch (CEPNotFoundException e) {
                    logger.error("A CEP with the specified URi is not yet instantiated.", e);
                    message = "A CEP with the specified URi is not yet instantiated.";

                    response.type("application/json");
                    response.status(HttpStatus.NOT_FOUND_404);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                } catch (DuplicationException e){
                    logger.error("A stream with the same name is already registered.", e);
                    message = "A stream with the same name is already registered.";

                    response.type("application/json");
                    response.status(HttpStatus.CONFLICT_409);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                } catch (Exception e){
                    e.printStackTrace();

                    logger.error("Error while creating new stream", e);
                    message = "Error while creating new stream. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/streams", (request, response) -> {

                String message;

                try {
                    Collection<StreamDescriptorIP> ets = streamList.values();
                    ArrayList<StreamDescriptor> streams = new ArrayList<>();

                    for(StreamDescriptorIP sdi : ets){
                        StreamDescriptor sd = new StreamDescriptor();
                        sd.setCepURI(sdi.getCepURI());
                        sd.setName(sdi.getSm().getStream().getName());
                        sd.setType(sdi.getSm().getStream().getUnderlyingType().getTypeName());
                        for(String s : sdi.getSm().getStream().getPropertyNames())
                            sd.addField(s, sdi.getSm().getStream().getPropertyType(s).getTypeName());
                        streams.add(sd);
                    }

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, streams));
                } catch (Exception e) {
                    logger.error("Error while retrieving streams info", e);
                    message = "Error while retrieving streams info. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/streams", (request, response) -> {
                response.type("application/json");

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    if (cepList.get(config.get("cepURI").toString()) == null)
                        throw new CEPNotFoundException("A CEP with the specified URi is not yet instantiated.");
                    EPServiceProvider cep = cepList.get(config.get("cepURI").toString()).getCep();

                    cep.getEPAdministrator().getConfiguration().removeEventType(config.get("eventName").toString(), config.get("force").toBoolean());
                    streamList.remove(config.get("eventName").toString());
                    message = "Stream " + config.get("eventName").toString() + " succesfully deleted";
                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (CEPNotFoundException e) {
                    logger.error("A CEP with the specified URi is not yet instantiated.", e);
                    message = "A CEP with the specified URi is not yet instantiated.";

                    response.type("application/json");
                    response.status(HttpStatus.NOT_FOUND_404);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                } catch (Exception e){
                    logger.error("Error while deleting stream", e);
                    message = "Error while creating deleting stream. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/streams/:name", (request, response) -> {
                response.type("application/json");

                String message;

                try {
                    StreamDescriptorIP sdi = streamList.get(request.params(":name"));

                    if(sdi != null) {

                        StreamDescriptor sd = new StreamDescriptor();
                        sd.setCepURI(sdi.getCepURI());
                        sd.setName(sdi.getSm().getStream().getName());
                        sd.setType(sdi.getSm().getStream().getUnderlyingType().getTypeName());
                        for(String s : sdi.getSm().getStream().getPropertyNames())
                            sd.addField(s, sdi.getSm().getStream().getPropertyType(s).getTypeName());

                        response.type("application/json");
                        response.status(HttpStatus.OK_200);

                        return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS,sd));
                    } else {
                        response.type("application/json");
                        response.status(HttpStatus.OK_200);

                        return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, new StreamDescriptor()));
                    }
                } catch (Exception e) {
                    logger.error("Error while retrieving stream info", e);
                    message = "Error while retrieving stream info. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/streams/:name", (request, response) -> {
                response.type("application/json");

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    if (cepList.get(config.get("cepURI").toString()) == null)
                        throw new CEPNotFoundException("A CEP with the specified URi is not yet instantiated.");
                    EPServiceProvider cep = cepList.get(config.get("cepURI").toString()).getCep();

                    cep.getEPAdministrator().getConfiguration().removeEventType(request.params(":name"), config.get("force").toBoolean());
                    streamList.remove(request.params(":name"));
                    message = "Stream " + request.params(":name") + " succesfully deleted";
                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (CEPNotFoundException e) {
                    logger.error("A CEP with the specified URi is not yet instantiated.", e);
                    message = "A CEP with the specified URi is not yet instantiated.";

                    response.type("application/json");
                    response.status(HttpStatus.NOT_FOUND_404);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                } catch (Exception e){
                    logger.error("Error while deleting stream", e);
                    message = "Error while creating deleting stream. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            //EPL

            post("/epl", (request, response) -> {
                response.type("application/json");

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    if (cepList.get(config.get("cepURI").toString()) == null)
                        throw new CEPNotFoundException("A CEP with the specified URi is not yet instantiated.");
                    EPServiceProvider cep = cepList.get(config.get("cepURI").toString()).getCep();

                    EPStatement cepStatement = cep.getEPAdministrator().createEPL(config.get("eplQuery").toString());

                    queryList.put(
                            cepStatement.getName(),
                            new QueryDescriptorIP(
                                    config.get("cepURI").toString(),
                                    cepStatement));

                    message = "EPL statement " + cepStatement.getName() + " succesfully registered";
                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

//                    TimeUnit.MILLISECONDS.sleep(2000);

                    return JsonStream.serialize(
                            new StandardResponse(
                                    StatusResponse.SUCCESS,
                                    message,
                                    new QueryDescriptor(
                                            config.get("cepURI").toString(),
                                            cepStatement.getName(),
                                            cepStatement.getText(),
                                            cepStatement.getState().toString(),
                                            new ArrayList<>())));

                } catch (CEPNotFoundException e) {
                    logger.error("A CEP with the specified URi is not yet instantiated.", e);
                    message = "A CEP with the specified URi is not yet instantiated.";

                    response.type("application/json");
                    response.status(HttpStatus.NOT_FOUND_404);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                } catch (Exception e){
                    logger.error("Error while registering EPL statement", e);
                    message = "Error while registering EPL statement. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/epl", (request, response) -> {
                response.type("application/json");

                String message;

                try {
                    Collection<QueryDescriptorIP> qdList = queryList.values();
                    ArrayList<QueryDescriptor> queries = new ArrayList<>();

                    for ( QueryDescriptorIP qd : qdList){

                       ArrayList<ConsumerDescriptor> cm = new ArrayList<>();
                        for (ConsumerDescriptorIP cd : qd.getConsumers().values()){
                            cm.add(new ConsumerDescriptor(cd.getId(), cd.getEPLQueryName(), cd.getType()));
                        }

                        queries.add(
                                new QueryDescriptor(
                                        qd.getCepURI(),
                                        qd.getEpStatement().getName(),
                                        qd.getEpStatement().getText(),
                                        qd.getEpStatement().getState().toString(),
                                        cm));
                    }

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, queries));
                } catch (Exception e) {
                    logger.error("Error while retrieving EPL statements info", e);
                    message = "Error while retrieving EPL statements info. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            put("/epl", (request, response) -> {
                response.type("application/json");

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

                                response.type("application/json");
                                response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

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

                                response.type("application/json");
                                response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                                return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                            }
                            break;
                        }
                    }

                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e) {
                    logger.error("Error while modifying EPL statement state", e);
                    message = "Error while modifying EPL statement state. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/epl", (request, response) -> {
                response.type("application/json");

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    if (cepList.get(config.get("cepURI").toString()) == null)
                        throw new CEPNotFoundException("A CEP with the specified URi is not yet instantiated.");
                    EPServiceProvider cep = cepList.get(config.get("cepURI").toString()).getCep();

                    EPStatement eps = cep.getEPAdministrator().getStatement(config.get("statementName").toString());

                    queryList.remove(eps.getName());
                    eps.destroy();

                    message = eps.getName() + " EPL statement succesfully removed.";
                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (CEPNotFoundException e) {
                    logger.error("A CEP with the specified URi is not yet instantiated.", e);
                    message = "A CEP with the specified URi is not yet instantiated.";

                    response.type("application/json");
                    response.status(HttpStatus.NOT_FOUND_404);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                } catch (Exception e) {
                    logger.error("Error while removing EPL statement", e);
                    message = "Error while removing EPL statement. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            get("/epl/:name", (request, response) -> {
                response.type("application/json");

                String message;

                try {
                    QueryDescriptorIP qd = queryList.get(request.params(":name"));

                    ArrayList<ConsumerDescriptor> cm = new ArrayList<>();
                    for (ConsumerDescriptorIP cd : qd.getConsumers().values()){
                        cm.add(new ConsumerDescriptor(cd.getId(), cd.getEPLQueryName(), cd.getType()));
                    }

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(
                            new StandardResponse(StatusResponse.SUCCESS,
                                    new QueryDescriptor(
                                            qd.getCepURI(),
                                            qd.getEpStatement().getName(),
                                            qd.getEpStatement().getText(),
                                            qd.getEpStatement().getState().toString(),
                                            cm)));

                } catch (Exception e) {
                    logger.error("Error while retrieving ELP statement info", e);
                    message = "Error while retrieving ELP statement info. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            delete("/epl:name", (request, response) -> {
                response.type("application/json");

                String message;

                try {

                    Any config = JsonIterator.deserialize(request.body());

                    if (cepList.get(config.get("cepURI").toString()) == null)
                        throw new CEPNotFoundException("A CEP with the specified URi is not yet instantiated.");
                    EPServiceProvider cep = cepList.get(config.get("cepURI").toString()).getCep();


                    EPStatement eps = cep.getEPAdministrator().getStatement(request.params(":name"));

                    queryList.remove(eps.getName());
                    eps.destroy();

                    message = eps.getName() + " EPL statement succesfully removed.";
                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(StatusResponse.SUCCESS);
                } catch (CEPNotFoundException e) {
                    logger.error("A CEP with the specified URi is not yet instantiated.", e);
                    message = "A CEP with the specified URi is not yet instantiated.";

                    response.type("application/json");
                    response.status(HttpStatus.NOT_FOUND_404);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                } catch (Exception e) {
                    logger.error("Error while removing EPL statement", e);
                    message = "Error while removing EPL statement. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            put("/epl/:name", (request, response) -> {
                response.type("application/json");

                String message = new String();

                try {

                    Any config = JsonIterator.deserialize(request.body());
                    switch (config.get("action").toString().toLowerCase()) {
                        case "stop": {
                            QueryDescriptorIP qd = queryList.get(request.params(":name"));
                            if (qd.getEpStatement().getState().toString().equals("STARTED")) {
                                qd.getEpStatement().stop();
                                queryList.put(request.params(":name"), qd);
                                message = "The statement " + request.params(":name") + " is now " + qd.getEpStatement().getState().toString() + ".";
                            } else {
                                message = "The statement " + request.params(":name") + " is in " + qd.getEpStatement().getState().toString() + " state, and cannot be stopped.";
                                logger.info(message);

                                response.type("application/json");
                                response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                                return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                            }
                            break;
                        }
                        case "start": {
                            QueryDescriptorIP qd = queryList.get(request.params(":name"));
                            if (qd.getEpStatement().getState().toString().equals("STOPPED")) {
                                qd.getEpStatement().start();
                                queryList.put(request.params(":name"), qd);
                                message = "The statement " + request.params(":name") + " is now " + qd.getEpStatement().getState().toString() + ".";
                            } else {
                                message = "The statement " + request.params(":name") + " is in " + qd.getEpStatement().getState().toString() + " state, and cannot be started.";
                                logger.info(message);

                                response.type("application/json");
                                response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                                return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                            }
                            break;
                        }
                    }

                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e) {
                    logger.error("Error while modifying EPL statement state", e);
                    message = "Error while modifying EPL statement state. See log messages for further information.";
                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

            //CONSUMER

            post("/consumers", (request, response) -> {
                response.type("application/json");

                String message;

                try {
                    Any config = JsonIterator.deserialize(request.body());

                    Class<? extends Consumer> cc = cMap.get(config.get("type").toString());
                    Consumer c = cc.newInstance();
                    c.initialize(request.body());

                    QueryDescriptorIP qd = queryList.get(config.get("eplQueryName").toString());
                    qd.getEpStatement().addListener(c);
                    ConsumerDescriptorIP cd = new ConsumerDescriptorIP(c, c.getId().toString(), qd.getEpStatement().getName(), config.get("type").toString());
                    qd.addConsumer(cd);
                    queryList.put(config.get("eplQueryName").toString(), qd);
                    consumerList.put(c.getId().toString(), cd);

                    message = "Consumer " + config.get("type").toString() + " succesfully connected to the EPL statement " + config.get("eplQueryName").toString();
                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

//                    TimeUnit.MILLISECONDS.sleep(2000);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message, new ConsumerDescriptor(cd.getId(), cd.getEPLQueryName(), cd.getType())));
                } catch (Exception e) {
                    logger.error("Error while registering consumer", e);
                    message = "Error while registering consumer. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }

            });

            delete("/consumers/:id", (request, response) -> {
                response.type("application/json");

                String message;

                try {
                    ConsumerDescriptorIP cs = consumerList.get(request.params(":id"));
                    QueryDescriptorIP qd = queryList.get(cs.getEPLQueryName());
                    qd.getEpStatement().removeListener(cs.getConsumer());
                    queryList.put(qd.getEpStatement().getName(), qd);

                    message = "Consumer " + cs.getId() + " stop listening to the stream created by " + qd.getEpStatement().getName() + " EPL statement.";
                    logger.info(message);

                    response.type("application/json");
                    response.status(HttpStatus.OK_200);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.SUCCESS, message));
                } catch (Exception e) {
                    logger.error("Error while deleting consumer.", e);
                    message = "Error while deleting consumer. See log messages for further information.";

                    response.type("application/json");
                    response.status(HttpStatus.INTERNAL_SERVER_ERROR_500);

                    return JsonStream.serialize(new StandardResponse(StatusResponse.ERROR, message));
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
