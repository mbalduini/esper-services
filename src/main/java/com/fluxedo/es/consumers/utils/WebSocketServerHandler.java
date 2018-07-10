package com.fluxedo.es.consumers.utils;


import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import java.io.IOException;

/**
 * Created by Marco Balduini on 04/07/2018 as part of project esperservices.
 */
public final class WebSocketServerHandler extends Endpoint {

    @Override
    public void onOpen(final Session session, EndpointConfig config) {
        session.addMessageHandler(new MessageHandler.Whole<String>() {
            @Override
            public void onMessage(String message) {
                try {
                    for (Session s : session.getOpenSessions()) {
                        s.getBasicRemote().sendText(message);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
