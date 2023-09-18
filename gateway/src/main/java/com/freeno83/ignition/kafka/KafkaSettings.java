package com.freeno83.ignition.kafka;


import com.inductiveautomation.ignition.gateway.localdb.persistence.PersistentRecord;
import com.inductiveautomation.ignition.gateway.localdb.persistence.RecordMeta;

public class KafkaSettings extends PersistentRecord{

    public static final RecordMeta<KafkaSettings> META = new RecordMeta<>(KafkaSettings.class, "kafkasettings");

    @Override
    public RecordMeta<?> getMeta() {
        return META;
    }
}