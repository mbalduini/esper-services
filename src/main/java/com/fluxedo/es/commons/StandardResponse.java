package com.fluxedo.es.commons;

/**
 * Created by Marco Balduini on 18/06/2018 as part of project esperservices.
 */
public class StandardResponse {

    private StatusResponse status;
    private String message;
    private Object data;

    public StandardResponse(StatusResponse status) {
        this.status = status;
    }

    public StandardResponse(StatusResponse status, String message) {
        this.status = status;
        this.message = message;
    }

    public StandardResponse(StatusResponse status, Object data) {
        this.status = status;
        this.data = data;
    }

    public StandardResponse(StatusResponse status, String message, Object data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public StatusResponse getStatus() {
        return status;
    }

    public void setStatus(StatusResponse status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}


