package com.freeno83.ignition.kafka;

import com.inductiveautomation.ignition.client.gateway_interface.ModuleRPCFactory;

public class ClientScriptModule extends AbstractScriptModule{
    private final KafkaProducerRPCScriptHook rpc;

    public ClientScriptModule() {
        rpc = ModuleRPCFactory.create(
                "com.freeno83.ignition.kafka",
                KafkaProducerRPCScriptHook.class);
    }

    @Override
    protected int sendScriptingDataImp(String key, String value) {
        return rpc.sendScriptingData(key, value);
    }
}