package com.freeno83.ignition.kafka.web;

import com.freeno83.ignition.kafka.GatewayHook;
import com.freeno83.ignition.kafka.records.KafkaSettingsRecord;
import com.inductiveautomation.ignition.gateway.model.IgnitionWebApp;
import com.inductiveautomation.ignition.gateway.web.components.RecordEditForm;
import com.inductiveautomation.ignition.gateway.web.pages.IConfigPage;
import com.inductiveautomation.ignition.gateway.web.models.LenientResourceModel;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.wicket.Application;

public class KafkaSettingsPage extends RecordEditForm {
    public static final Pair<String, String> MENU_LOCATION =
            Pair.of(GatewayHook.CONFIG_CATEGORY.getName(), "kafka");

    public KafkaSettingsPage(final IConfigPage configPage){
        super(configPage, null, new LenientResourceModel("kafka.nav.settings.panelTitle"),
                ((IgnitionWebApp) Application.get()).getContext().getPersistenceInterface().find(KafkaSettingsRecord.META, 0L)
        );
    }

    @Override
    public Pair<String, String> getMenuLocation() {
        return MENU_LOCATION;
    }
}