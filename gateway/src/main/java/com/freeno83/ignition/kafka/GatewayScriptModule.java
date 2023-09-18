package com.freeno83.ignition.kafka;

import com.freeno83.ignition.kafka.datasink.*;
import com.freeno83.ignition.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.common.alarming.AlarmEvent;
import com.inductiveautomation.ignition.common.alarming.EventData;
import com.inductiveautomation.ignition.gateway.history.HistoryManager;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class GatewayScriptModule extends AbstractScriptModule{

    private final Logger logger = LoggerFactory.getLogger("Kafka." + getClass().getSimpleName());

    private HistoryManager historyManager;
    private KafkaSink tagSink, alarmSink, scriptSink;
    private GatewayContext context;
    private KafkaSettingsRecord kafkaConfig;
    private Map<String, BaseSink> dataSinksMap = new HashMap<>();
    private String alarmTopic, scriptingTopic;
    private String hostName;

    public GatewayScriptModule() {
    }

    public void setGatewayContext(GatewayContext ctx) {
        this.context = ctx;
        this.historyManager = ctx.getHistoryManager();
    }

    public Collection<BaseSink> getDataSinks() {
        return this.dataSinksMap.values();
    }

    public void shutDownSinks() {
        dataSinkOperation(SinkOps.Unregister);
        dataSinksMap.clear();
    }

    public void initializeDataSinks(KafkaSettingsRecord kafkaSettings) {
        this.kafkaConfig = kafkaSettings;

        // topics can be changed from the gateway UI
        this.alarmTopic = kafkaSettings.getAlarmsTopic();
        this.scriptingTopic = kafkaSettings.getScriptingTopic();

        shutDownSinks();
        boolean enabled = kafkaSettings.getEnabled();
        boolean brokerCheck = enabled ? brokersAreReachable(kafkaSettings) : false;

        if (enabled & brokerCheck) {
            this.tagSink = new TagSink(Constants.TAG_SINK_NAME, kafkaSettings);
            this.alarmSink = new KafkaSink(Constants.ALARM_SINK_NAME, kafkaSettings);
            this.scriptSink = new KafkaSink(Constants.SCRIPT_SINK_NAME, kafkaSettings);

            this.alarmSink.addStats(Constants.ALARM_SINK_NAME);
            this.scriptSink.addStats(Constants.SCRIPT_SINK_NAME);

            this.dataSinksMap.put(Constants.TAG_SINK_NAME, tagSink);
            this.dataSinksMap.put(Constants.ALARM_SINK_NAME, alarmSink);
            this.dataSinksMap.put(Constants.SCRIPT_SINK_NAME, scriptSink);

            dataSinkOperation(SinkOps.Register);
        } else {
            if (enabled) {
                logger.info("Unable to reach provided brokers, setting enabled to: false");
                kafkaSettings.setEnabled(false);
            }
        }

        String method;

        if (enabled) {
            if (kafkaSettings.getUseStoreAndfwd()) {
                method = "sendWithHistoryManager";
            } else method = "sendWithProducer";
        } else method = "doNothing";

        logger.info("Runtime method: " + method);

        try {
            this.hostName = InetAddress.getLocalHost().getHostName().toUpperCase();
        } catch (UnknownHostException e) {
            this.hostName = this.context.getSystemProperties().getSystemName();
        }
    }

    private void dataSinkOperation(SinkOps op) {
        if (op == SinkOps.Register) {
            for (BaseSink sink : this.dataSinksMap.values()) {
                this.historyManager.registerSink(sink);
            }
        }

        if (op == SinkOps.Unregister) {
            for (BaseSink sink : this.dataSinksMap.values()) {
                sink.closeProducer();
                this.historyManager.unregisterSink(sink, true);
            }
        }
    }

    public int sendWithHistoryManager(String sinkName, SinkData data) {
        try {
            this.historyManager.storeHistory(sinkName, data);
        } catch (Exception e) {
            logger.error("Error sending data with history manager: " + e.getCause().toString());
        }
        return 0;
    }

    public int sendWithProducer(String sinkName, SinkData data) throws IOException {
        BaseSink sink = this.dataSinksMap.get(sinkName);
        sink.sendPipelineDataWithProducer(data);
        sink.setLastMessageTime(data.getSignature());
        return 0;
    }

    public int doNothing(String sinkName) {
        logger.info("Kafka producer is disabled, data sink inactive: " + sinkName);
        return 2;
    }

    private Boolean nullOrMatchOne(String[] group, String one) {
        return group == null || Arrays.asList(group).stream().anyMatch(i -> one.matches(i.trim()));
    }

    public void sendEquipmentAlarm(AlarmEvent alarm, EventData data, KafkaSettingsRecord kafkaSettings) {

        int minimumPriority = kafkaSettings.getAlarmPriorityInt();
        Boolean hasPriority = alarm.getPriority().ordinal() >= minimumPriority;
        String src = String.valueOf(alarm.getSource()), path = String.valueOf(alarm.getDisplayPath());
        String[] srcFilter = kafkaSettings.getSource(), pathFilter = kafkaSettings.getDispPath(), srcPathFilter = kafkaSettings.getSrcPath();
        Boolean isSource = nullOrMatchOne(srcFilter, src);
        Boolean isPath = nullOrMatchOne(pathFilter, path);
        Boolean isPathOrSource = nullOrMatchOne(srcPathFilter, path) || nullOrMatchOne(srcPathFilter, src);

        String provider = src.split(":")[1];
        String tagPath = src.split("tag:")[1];

        if (hasPriority && isSource && isPath & isPathOrSource) {
            try {
                String json = new JSONObject()
                        .put("uuid", alarm.getId().toString())
                        .put("gatewayName", this.hostName)
                        .put("provider", provider)
                        .put("tagPath", tagPath)
                        .put("displayPath", path)
                        .put("priority", alarm.getPriority().ordinal())
                        .put("eventType", alarm.getState().ordinal())
                        .put("eventData", data)
                        .put("eventFlags", alarm.getLastEventState().ordinal())
                        .put("timestamp", data.getTimestamp())
                        .toString();
                SinkData toSend = new SinkData(alarmTopic, json, Constants.ALARM_SINK_NAME);
                sendKafkaData(toSend, Constants.ALARM_SINK_NAME);
            } catch (JSONException e) {
                logger.error("Error sending alarm data: " + e.getCause());
                this.alarmSink.addOneFailedCount(Constants.ALARM_SINK_NAME);
            }
        }
    }

    private void sendKafkaData(SinkData data, String sink) {
        try {
            if (kafkaConfig.getEnabled()) {
                if (kafkaConfig.getUseStoreAndfwd()) {
                    sendWithHistoryManager(sink, data);
                } else {
                    sendWithProducer(sink, data);
                }
            } else {
                doNothing(sink);
            }
        } catch (Exception e) {
            logger.error(String.format("Can't send data due to error: %s", e));
            BaseSink sinkToUse = this.dataSinksMap.get(sink);
            sinkToUse.addOneFailedCount(data.getSignature());
        }
    }

    @Override
    protected int sendScriptingDataImp(String key, String value) {
        return sendScriptingData(scriptingTopic, Constants.SCRIPT_SINK_NAME, key, value);
    }

    protected int sendScriptingData(String topic, String sink, String key, String value) {
        if ( kafkaConfig.getScriptingEnabled() & kafkaConfig.getEnabled()) {
            try {
                String json = new JSONObject()
                        .put("timestamp", System.currentTimeMillis())
                        .put("gatewayName", this.hostName)
                        .put("key", key)
                        .put("value", value)
                        .toString();
                SinkData toSend = new SinkData(topic, json, sink);
                sendKafkaData(toSend, sink);
                return 1;
            } catch (Exception e) {
                logger.error(String.format("Can't send scripting data due to error: %s",  e));
                return 0;
            }
        } else { return 0; }
    }

    public Boolean brokersAreReachable(KafkaSettingsRecord kafkaSettings) {
        Boolean reachable = false;
        AdminClient client = null;
        logger.info("Checking if the provided brokers can be reached");

        try {
            Properties props = KafkaProps.getConsumerProps(kafkaSettings);
            client = AdminClient.create(props);
            Collection nodes = client.describeCluster().nodes().get();
            reachable = nodes != null && nodes.size() > 0;
            if (reachable) {
                logger.info("Successfully contacted brokers");
                logger.info("Closing admin client");
                client.close();
            }
        } catch ( Exception e) {
            logger.error(String.format("Can't reach brokers due to error: %s", e));
        }
        return reachable;
    }
}