package com.freeno83.ignition.kafka;

import com.inductiveautomation.ignition.common.script.hints.ScriptFunctionDocProvider;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public class ScriptDocs implements ScriptFunctionDocProvider {
    @Override
    public String getMethodDescription(String s, Method method) {
        if( method.getName().equals("sendScriptingData")) {
            return "Send scripting data to the topic specified in the gateway";}
        return null;
    }

    @Override
    public Map<String, String> getParameterDescriptions(String s, Method method) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        if( method.getName().equals("sendScriptingData") ) {
            map.put("key", "Unique identifier for the data source");
            map.put("value", "Stringified JSON object or String");
        }
        return map;
    }

    @Override
    public String getReturnValueDescription(String s, Method method) {
        if( method.getName().equals("sendScriptingData") ) {
            return "1 for success or 0 in case an issue occurred";
        }
        return null;
    }
}