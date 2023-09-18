package com.freeno83.ignition.kafka.web;

import com.freeno83.ignition.kafka.datasink.BaseSink;
import com.freeno83.ignition.kafka.datasink.MessageStats;
import com.freeno83.ignition.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.gateway.dataroutes.RequestContext;
import com.inductiveautomation.ignition.gateway.dataroutes.RouteGroup;
import com.inductiveautomation.ignition.gateway.dataroutes.WicketAccessControl;
import com.inductiveautomation.ignition.gateway.localdb.persistence.PersistenceSession;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import simpleorm.dataset.SQuery;

import javax.servlet.http.HttpServletResponse;
import java.util.Collection;

public class KafkaStatusRoutes {

    private RouteGroup routes;
    private Collection<BaseSink> sinks;

    public KafkaStatusRoutes(Collection<BaseSink> dataSinks, RouteGroup group) {
        this.routes = group;
        this.sinks = dataSinks;
    }

    public void mountRoutes() {
        routes.newRoute("/status/connections")
                .handler(this::getConnectionsStatus)
                .type(RouteGroup.TYPE_JSON)
                .restrict(WicketAccessControl.STATUS_SECTION)
                .mount();
    }

    public JSONObject getConnectionsStatus(RequestContext requestContext, HttpServletResponse httpServletResponse) throws JSONException {
        GatewayContext context = requestContext.getGatewayContext();
        JSONObject json = new JSONObject();
        PersistenceSession session = context.getPersistenceInterface().getSession();

        try {
            SQuery<KafkaSettingsRecord> query = new SQuery<>(KafkaSettingsRecord.META);
            KafkaSettingsRecord config = session.queryOne(query);

            if (config != null){
                json.put("count", String.valueOf(this.sinks.size()));
                json.put("Enabled", config.getEnabled());
                json.put("UseStoreAndForward", config.getUseStoreAndfwd());
                json.put("AlarmsEnabled", config.getAlarmsEnabled());
                json.put("ScriptingEnabled", config.getScriptingEnabled());
                JSONArray jsonArray = new JSONArray();
                json.put("connections", jsonArray);

                JSONObject connectionJson = new JSONObject();
                jsonArray.put(connectionJson);
                connectionJson.put("Brokers", config.getBrokerList());
                connectionJson.put("TagHistoryTopic", config.getTagHistoryTopic());
                connectionJson.put("isSSL", config.getUseSSL());
                connectionJson.put("AlarmsTopic", config.getAlarmsTopic());
                connectionJson.put("MinimumPriority", config.getAlarmPriorityString());
                connectionJson.put("Source", config.getSource());
                connectionJson.put("DispPath", config.getDispPath());
                connectionJson.put("SrcPath", config.getSrcPath());
                connectionJson.put("ScriptingTopic", config.getScriptingTopic());

                JSONArray sinksArray = new JSONArray();
                json.put("sinks", sinksArray);

                for (BaseSink sink : this.sinks) {
                    JSONObject sinkJson = new JSONObject();
                    sinksArray.put(sinkJson);
                    sinkJson.put("name", sink.getPipelineName());

                    JSONArray statsArray = new JSONArray();
                    sinkJson.put("stats", statsArray);

                    for (MessageStats stats : sink.getStats()) {
                        JSONObject statJson = new JSONObject();
                        statsArray.put(statJson);
                        statJson.put("MessageCount", stats.getSuccessCount());
                        statJson.put("FailedCount", stats.getFailedCount());
                        statJson.put("Source", stats.getSourceName());
                        statJson.put("LastMessageTime", stats.getLastMessageTime());
                        statJson.put("Started", sink.getLiveTime());
                        statJson.put("LifeSpan", sink.getLifeSpanDays());
                    }
                }
            }
        } finally {
            session.close();
        }
        return json;
    }
}