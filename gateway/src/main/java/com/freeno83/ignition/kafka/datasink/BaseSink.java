package com.freeno83.ignition.kafka.datasink;

import com.freeno83.ignition.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.gateway.history.DataSinkInformation;
import com.inductiveautomation.ignition.gateway.history.HistoricalData;
import com.inductiveautomation.ignition.gateway.history.HistoryFlavor;
import com.inductiveautomation.ignition.gateway.history.sf.BasicDataTransaction;
import com.inductiveautomation.ignition.gateway.history.sf.sinks.AbstractSink;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseSink extends AbstractSink {

    protected LocalDateTime liveTime;
    protected Map<String, MessageStats> stats;
    protected String name;
    protected final Logger logger = LoggerFactory.getLogger("Kafka." + getClass().getSimpleName());
    protected KafkaSettingsRecord config;
    protected GatewayContext context;
    protected String hostName;

    public BaseSink(String pipelineName) {
        super(pipelineName);
        this.name = pipelineName;
        stats = new HashMap<>();

        // This is in place to ensure consistent gateway names
        try {
            this.hostName = InetAddress.getLocalHost().getHostName().toUpperCase();
        } catch (UnknownHostException e) {
            this.hostName = context.getSystemProperties().getSystemName();
        }
    }

    @Override
    public List<DataSinkInformation> getInfo() {
        //TODO;
        return null;
    }

    public abstract void resetProducer(KafkaSettingsRecord kafkaSettings);
    public abstract void closeProducer();
    public abstract void sendDataWithProducer(SinkData data) throws IOException;
    public abstract void sendPipelineDataWithProducer(SinkData data) throws IOException;
    public void storeData(HistoricalData data) throws IOException {

        for(HistoricalData row : BasicDataTransaction.class.cast(data).getData()) {
            try {
                SinkData value = SinkData.class.cast(row);
                sendPipelineDataWithProducer(value);
                this.stats.get(row.getSignature()).setLastMessageTime();
            } catch (Exception e) {
                logger.error(e.toString());
            }
        }
    }

    @Override
    public boolean acceptsData(HistoryFlavor dataType) {
        return true;
    }

    @Override
    public boolean isLicensedFor(HistoryFlavor dataType) {
        return true;
    }

    public String getLiveTime() {
        return liveTime.toString();
    }

    public Boolean isSSL() {
        return this.config.getUseSSL();
    }

    public String getBrokerEndpoints() {
        return this.config.getBrokerList();
    }

    public Collection<MessageStats> getStats() {
        return this.stats.values();
    }

    public long getLifeSpanDays() {
        return ChronoUnit.DAYS.between(liveTime, LocalDateTime.now());
    }

    public void resetStats() {
        this.liveTime = LocalDateTime.now();
        for (MessageStats stat : this.stats.values()) {
            stat.reset();
        }
    }

    public void setLastMessageTime(String source) {
        this.stats.get(source).setLastMessageTime();
    }

    public void addOneFailedCount(String source) {
        this.stats.get(source).addOneFailedCount();
    }

    public void addOneSuccessCount(String source) {
        this.stats.get(source).addOneSuccessCount();
    }
}