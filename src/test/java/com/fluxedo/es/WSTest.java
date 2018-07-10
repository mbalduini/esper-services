package com.fluxedo.es;

import com.fluxedo.es.consumers.utils.WebSocketServerHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;

import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig;

/**
 * Created by Marco Balduini on 04/07/2018 as part of project esperservices.
 */
public class WSTest {

    public static void main(String[] args) {
        try {

            Server server = new Server();
            ServerConnector connector = new ServerConnector(server);
            connector.setPort(3456);
            server.addConnector(connector);

            // Setup the basic application "context" for this application at "/"
            // This is also known as the handler tree (in jetty speak)
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");
            server.setHandler(context);

            // Initialize javax.websocket layer
            ServerContainer wscontainer = WebSocketServerContainerInitializer.configureContext(context);
            server.start();
//            server.dump(System.err);
//            server.join();

            ServerEndpointConfig sec = ServerEndpointConfig.Builder.create(WebSocketServerHandler.class, "/test").build();
            wscontainer.addEndpoint(sec);

            System.out.println("test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
