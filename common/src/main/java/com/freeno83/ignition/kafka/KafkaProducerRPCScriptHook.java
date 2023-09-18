package com.freeno83.ignition.kafka;

public interface KafkaProducerRPCScriptHook {
    int sendScriptingData(String key, String value);
}