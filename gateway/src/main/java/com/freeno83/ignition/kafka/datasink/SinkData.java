package com.freeno83.ignition.kafka.datasink;

import com.inductiveautomation.ignition.gateway.history.DatasourceData;
import com.inductiveautomation.ignition.gateway.history.HistoricalData;
import com.inductiveautomation.ignition.gateway.history.HistoryFlavor;

public class SinkData implements HistoricalData {

    private String topic, value, signature;

    public SinkData(String topic, String value,  String signature) {
        this.topic = topic;
        this.value = value;
        this.signature = signature;
    }

    @Override
    public String toString() {
        return "Topic: " + topic +  " Value: " + value;
    }

    @Override
    public String getSignature() {
        return this.signature;
    }

    @Override
    public String getLoggerName() {
        return "Kafka." + this.signature;
    }

    @Override
    public HistoryFlavor getFlavor() {
        return DatasourceData.FLAVOR;
    }

    @Override
    public int getDataCount() {
        return 1;
    }

    public String getTopic() {
        return topic;
    }

    public String getValue() {
        return this.value;
    }
}