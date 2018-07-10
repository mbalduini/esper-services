package com.fluxedo.es.consumers;

import com.espertech.esper.client.EventBean;
import com.fluxedo.es.RESTServer;
import com.fluxedo.es.commons.Consumer;
import com.fluxedo.es.consumers.utils.WebSocketServerHandler;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.*;
import javax.websocket.server.ServerEndpointConfig;
import java.io.IOException;
import java.net.URI;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class WebSocketConsumer extends Consumer{

    private String wsEndpointName;
    private String wsServerBaseUri;
    private Session session;
    private Logger logger = LoggerFactory.getLogger(WebSocketConsumer.class);
    private boolean sampling = true;

    public WebSocketConsumer() {
        super();
    }

    public void initialize(String configuration){

        Any config = JsonIterator.deserialize(configuration);
        Any wsConfig = config.get("wsConfig");

        sampling = config.get("sampling").toBoolean();

        try {

            this.wsServerBaseUri = wsConfig.get("wsServerBaseUri").toString();
            this.wsEndpointName = wsConfig.get("wsEndpointName").toString();

            if(!this.wsEndpointName.startsWith("/"))
                this.wsEndpointName = "/" + this.wsEndpointName;

            ServerEndpointConfig sec = ServerEndpointConfig.Builder.create(WebSocketServerHandler.class, wsEndpointName).build();
            RESTServer.wscontainer.addEndpoint(sec);

            URI echoUri = new URI(wsServerBaseUri + wsEndpointName);

            final ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();

            WebSocketContainer wsc = ContainerProvider.getWebSocketContainer();
            session = wsc.connectToServer(new Endpoint() {
                @Override
                public void onOpen(final Session session, EndpointConfig config) {
                    session.addMessageHandler(new MessageHandler.Whole<String>() {
                        @Override
                        public void onMessage(String message) {
                        }
                    });
                }
            }, cec, echoUri);


            logger.info("Connecting to : {}", echoUri);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void update(EventBean[] newData, EventBean[] oldData) {

        StringBuffer sb;

        for (EventBean e : newData) {
            if(e != null) {
                try {
                    sb = new StringBuffer();
                    for (String s : e.getEventType().getPropertyNames()) {
                        sb.append(s + " : " + e.get(s).toString() + ",");
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    if(sampling)
                        sb.append("\nPlus other " + (newData.length - 1) + " similar events\n");
                    session.getBasicRemote().sendText(sb.toString());
                } catch (IOException e1) {
                    logger.error("Error while sending message on the websocket", e1);
                }
            }
            if(sampling)
                break;
        }
    }
}
