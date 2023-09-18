package com.freeno83.ignition.kafka.datasink;

import com.freeno83.ignition.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.gateway.history.HistoricalData;
import com.inductiveautomation.ignition.gateway.history.HistoricalTagValue;
import com.inductiveautomation.ignition.gateway.history.sf.BasicDataTransaction;
import com.inductiveautomation.ignition.gateway.sqltags.model.BasicScanclassHistorySet;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TagSink extends KafkaSink{
    private String topic;
    private final Logger logger = LoggerFactory.getLogger("Kafka." + getClass().getSimpleName());

    public TagSink(String pipeLineName, KafkaSettingsRecord kafkaSettings) {
        super(pipeLineName, kafkaSettings);
        // topic can be changed from the gateway UI
        this.topic = kafkaSettings.getTagHistoryTopic();
        this.stats.put(pipeLineName, new MessageStats(pipeLineName));
    }

    @Override
    public void storeData(HistoricalData data) throws IOException {
        int pathIndex = 0;

        for (HistoricalData row : BasicDataTransaction.class.cast(data).getData()) {
            // prevent TagRetirementList from going to store and forward quarantine
            if (row instanceof BasicScanclassHistorySet) {
                BasicScanclassHistorySet scanset = BasicScanclassHistorySet.class.cast(row);
                if (scanset.size() == 0) continue;

                String gatewayName = this.hostName;
                String provider = scanset.getProviderName();
                pathIndex = provider.length() + 2;

                for (HistoricalTagValue tagValue : scanset) {
                    try{
                        String json = new JSONObject()
                                .put("gatewayName", gatewayName)
                                .put("provider", provider)
                                .put("tagPath", tagValue.getSource().toString().replace("["+provider+"]", ""))
                                .put("quality", tagValue.getQuality())
                                .put("value", String.valueOf(tagValue.getValue()))
                                .put("timestamp", tagValue.getTimestamp().getTime())
                                .toString();

                        SinkData value = new SinkData(topic, json, this.getPipelineName());
                        this.sendDataWithProducer(value);

                    } catch (JSONException e) {
                        logger.error("Error sending tag: " + e);
                    }
                }
            } else {
                logger.info("Not storing data with the following class: " + row.getClass().toString());
            };
        }
        if (pathIndex > 0) {
            setLastMessageTime(this.name);
        }
    }
}