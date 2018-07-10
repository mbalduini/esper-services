package com.fluxedo.es.throughputTest.MQTT.bremboEvent;

/**
 * Created by Marco Balduini on 23/05/2018 as part of project brembo_poc.
 */
public class BremboEvent {
    private int varId;
    private double value;
    private String devSn;
    private String onTime;
    private long unixTs;

    public BremboEvent() {
        super();
    }

    public BremboEvent(int varId, double value, String devSn, String onTime, long unixTs) {
        this.varId = varId;
        this.value = value;
        this.devSn = devSn;
        this.onTime = onTime;
        this.unixTs = unixTs;
    }

    public int getVarId() {
        return varId;
    }

    public void setVarId(int varId) {
        this.varId = varId;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getDevSn() {
        return devSn;
    }

    public void setDevSn(String devSn) {
        this.devSn = devSn;
    }

    public String getOnTime() {
        return onTime;
    }

    public void setOnTime(String onTime) {
        this.onTime = onTime;
    }

    public long getUnixTs() {
        return unixTs;
    }

    public void setUnixTs(long unixTs) {
        this.unixTs = unixTs;
    }

    @Override
    public String toString() {
        return devSn + "," + onTime + "," + varId + "," + value + "," + unixTs;
    }
}
