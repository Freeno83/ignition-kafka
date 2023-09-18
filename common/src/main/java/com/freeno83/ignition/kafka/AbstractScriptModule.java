package com.freeno83.ignition.kafka;

public abstract class AbstractScriptModule implements KafkaProducerRPCScriptHook{

    @Override
    public int sendScriptingData(String key, String value) {
        return sendScriptingDataImp(key, value);
    }
    protected abstract int sendScriptingDataImp(String key, String value);

}