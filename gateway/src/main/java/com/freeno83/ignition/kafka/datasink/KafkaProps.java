package com.freeno83.ignition.kafka.datasink;

import com.freeno83.ignition.kafka.Constants;
import com.freeno83.ignition.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class KafkaProps {
    private static GatewayContext context;
    private static Logger logger = LoggerFactory.getLogger("Kafka");

    public static Properties getProducerProps(KafkaSettingsRecord kafkaSettings) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSettings.getBrokerList());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer settings
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Constants.IDEMPOTENCE);
        props.setProperty(ProducerConfig.ACKS_CONFIG, Constants.ACKS_CONFIG);
        props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Constants.MAX_INFLIGHT_REQUESTS);

        if (kafkaSettings.getUseSSL()) {
            return getSSLProps(props);
        } else { return props; }
    }

    public static Properties getConsumerProps(KafkaSettingsRecord kafkaSettings) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSettings.getBrokerList());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, Constants.TIMEOUT_MS);
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, Constants.MAX_IDLE_MS);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, Constants.RECONNECT_WAIT);

        if (kafkaSettings.getUseSSL()) {
            return getSSLProps(props);
        } else { return props; }
    }

    private static Properties getSSLProps(Properties props){
        String homePath = getGatewayHome();
        String sep = File.separator;

        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, Constants.SSL);
        props.put("security.protocol", Constants.SSL);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,"");

        // Keystore settings
        String keystorePath = homePath+String.format(Constants.SSL_CERT_PATH,sep,sep);
        if(fileExists(keystorePath)) {
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorePath);
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Constants.APP_NAME);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,Constants.APP_NAME);
        }
        // Truststore settings
        String truststorePath = homePath+String.format(Constants.TRUSTSTORE_PATH,sep,sep,sep);

        if (fileExists(truststorePath)) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Constants.APP_NAME);
        }
        return props;
    }

    private static String getGatewayHome(){
        String absPath = context.getSystemManager().getDataDir().getAbsolutePath();
        return absPath.substring(0,absPath.lastIndexOf(File.separator));
    }

    private static boolean fileExists(String path){
        File fObj = new File(path);
        return fObj.exists();
    }
}