package com.freeno83.ignition.kafka.datasink;

import com.freeno83.ignition.kafka.records.KafkaSettingsRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaSink extends BaseSink {

    private static Logger logger = LoggerFactory.getLogger("Kafka");
    private KafkaProducer<String, String> producer;

    public KafkaSink(String pipelineName, KafkaSettingsRecord kafkaSettings) {
        super(pipelineName);
        this.config = kafkaSettings;
        resetProducer(kafkaSettings);
    }

    public void addStats(String signature) {
        this.stats.put(signature, new MessageStats(signature));
    }

    @Override
    public void sendDataWithProducer(SinkData value) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(value.getTopic(), value.getValue());
        sendProducerData(value.getSignature(), record);
    }

    @Override
    public void sendPipelineDataWithProducer(SinkData data) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(data.getTopic(), data.getValue());
        sendProducerData(data.getSignature(), record);
    }

    private void sendProducerData(String signature, ProducerRecord<String, String> record) {
        try {
            this.producer.send(record);
            this.addOneSuccessCount(signature);
        } catch (Exception e) {
            logger.warn(String.format("Data (%s) was not sent due to error: %s", record, e));
            this.addOneFailedCount(signature);
        }
    }

    public void resetProducer(KafkaSettingsRecord kafkaSettings) {
        Thread.currentThread().setContextClassLoader(null);
        Properties props = KafkaProps.getProducerProps(kafkaSettings);
        this.producer = new KafkaProducer<>(props);
        this.resetStats();
    }

    @Override
    public void closeProducer() {
        if (this.producer != null) {
            logger.info("Closing producer for sink " + getPipelineName());
            this.producer.close();
        }
    }
}