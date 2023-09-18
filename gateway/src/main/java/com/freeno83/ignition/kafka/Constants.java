package com.freeno83.ignition.kafka;

public class Constants {
    public static final String DEFAULT_BROKERS = "127.0.0.1:9092";
    public static final String DEFAULT_TAG_HISTORY_TOPIC = "ignition-tag-history";
    public static final String DEFAULT_ALARM_EVENT_TOPIC = "ignition-alarm-events";
    public static final String DEFAULT_SCRIPTING_TOPIC = "ignition-script-events";
    public static final String TAG_SINK_NAME = "kafka-tag-history";
    public static final String ALARM_SINK_NAME = "kafka-alarm-events";
    public static final String SCRIPT_SINK_NAME = "kafka-script-events";
    public static final String APP_NAME = "ignition";
    public static final String IDEMPOTENCE = "true";
    public static final String ACKS_CONFIG = "all";
    public static final Integer TIMEOUT_MS = 10000;
    public static final Integer MAX_IDLE_MS = 5000;
    public static final Integer RECONNECT_WAIT = 10000;
    public static final String MAX_INFLIGHT_REQUESTS = "5";
    public static final String SSL = "SSL";
    public static final String SSL_CERT_PATH = "%swebserver%sssl.pfx";
    public static final String TRUSTSTORE_PATH = "%sdata%scertificates%struststore.jks";
}
