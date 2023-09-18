package com.freeno83.ignition.kafka;

import com.freeno83.ignition.kafka.records.KafkaSettingsRecord;
import com.freeno83.ignition.kafka.web.KafkaSettingsPage;
import com.freeno83.ignition.kafka.web.KafkaStatusRoutes;
import com.inductiveautomation.ignition.common.BundleUtil;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.alarming.AlarmEvent;
import com.inductiveautomation.ignition.common.alarming.AlarmListener;
import com.inductiveautomation.ignition.common.licensing.LicenseState;
import com.inductiveautomation.ignition.common.script.ScriptManager;
import com.inductiveautomation.ignition.common.util.LogUtil;
import com.inductiveautomation.ignition.common.util.LoggerEx;
import com.inductiveautomation.ignition.gateway.alarming.AlarmManager;
import com.inductiveautomation.ignition.gateway.clientcomm.ClientReqSession;
import com.inductiveautomation.ignition.gateway.dataroutes.RouteGroup;
import com.inductiveautomation.ignition.gateway.localdb.persistence.IRecordListener;
import com.inductiveautomation.ignition.gateway.model.AbstractGatewayModuleHook;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.web.components.AbstractNamedTab;
import com.inductiveautomation.ignition.gateway.web.models.*;
import com.inductiveautomation.ignition.gateway.web.pages.BasicReactPanel;
import com.inductiveautomation.ignition.gateway.web.pages.status.StatusCategories;
import org.apache.wicket.markup.html.WebMarkupContainer;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


public class GatewayHook extends AbstractGatewayModuleHook {
    private GatewayContext context;
    private final LoggerEx log = LogUtil.getLogger(getClass().getSimpleName());
    private final GatewayScriptModule scriptModule = new GatewayScriptModule();
    private AlarmListener alarmListener;
    private QualifiedPath alarmFilter;

    /** Status Panel Setup */
    private static final INamedTab HCE_STATUS_PAGE = new AbstractNamedTab(
            "kafka",
            StatusCategories.SYSTEMS,
            "kafka.nav.status.header") {

        @Override
        public WebMarkupContainer getPanel(String panelId) {
            // We've set  GatewayHook.getMountPathAlias() to return kafka, so we need to use that alias here.
            return new BasicReactPanel(panelId, "/res/kafka/js/kafkastatus.js", "kafkastatus");
        }

        @Override
        public Iterable<String> getSearchTerms(){
            return Arrays.asList("kafka", "Kafka");
        }
    };

    /** Config Panel Setup */

    public static final ConfigCategory CONFIG_CATEGORY =
            new ConfigCategory("kafka", "kafka.nav.header", 700);

    @Override
    public List<ConfigCategory> getConfigCategories() {
        return Collections.singletonList(CONFIG_CATEGORY);
    }

    public static final IConfigTab HCE_CONFIG_ENTRY = DefaultConfigTab.builder()
            .category(CONFIG_CATEGORY)
            .name("kafka")
            .i18n("kafka.nav.settings.title")
            .page(KafkaSettingsPage.class)
            .terms("kafka settings")
            .build();

    @Override
    public List<? extends IConfigTab> getConfigPanels() {
        return Collections.singletonList(
                HCE_CONFIG_ENTRY
        );
    }

    @Override
    public void setup(GatewayContext gatewayContext) {
        this.context = gatewayContext;
        log.debug("Beginning setup of Kafka module");

        try {
            BundleUtil.get().addBundle("kafka", getClass(), "kafka");
        } catch (Exception e) {
            log.error("Error Creating Bundle: ", e);
        }

        verifySchema(context);
        maybeCreateKafkaSettings(context);
        KafkaSettingsRecord kafkaSettings = context.getLocalPersistenceInterface().find(KafkaSettingsRecord.META, 0L);

        scriptModule.setGatewayContext(context);
        scriptModule.initializeDataSinks(kafkaSettings);
        setupAlarmManager(kafkaSettings);

        //Listen for updates to the settings record
        KafkaSettingsRecord.META.addRecordListener(new IRecordListener<KafkaSettingsRecord>() {
            @Override
            public void recordUpdated(KafkaSettingsRecord newConfig) {
                // check if the brokers are reachable, if not set enabled to false
                if (newConfig.getEnabled()) {
                    if (!scriptModule.brokersAreReachable(newConfig)) {
                        log.info("Unable to reach provided brokers, setting enabled to: false");
                        newConfig.setEnabled(false);
                    }
                }
                scriptModule.initializeDataSinks(newConfig);
                context.getLocalPersistenceInterface().save(newConfig);
                setupAlarmManager(newConfig);
                log.info("Kafka settings updated");
            }

            @Override
            public void recordAdded(KafkaSettingsRecord kafkaSettingsRecord) {
                log.info("Record Added()");
            }

            @Override
            public void recordDeleted(KeyValue keyValue) {
                log.info("Record Deleted()");
            }
        });

        log.debug("Setup Complete");
    }

    private void setupAlarmManager(KafkaSettingsRecord config) {
        alarmListener = new AlarmListener() {
            @Override
            public void onActive(AlarmEvent alarmEvent) {
                scriptModule.sendEquipmentAlarm(alarmEvent, alarmEvent.getActiveData(), config);
            }
            @Override
            public void onClear(AlarmEvent alarmEvent) {
                scriptModule.sendEquipmentAlarm(alarmEvent, alarmEvent.getClearedData(), config);
            }
            @Override
            public void onAcknowledge(AlarmEvent alarmEvent) {
                scriptModule.sendEquipmentAlarm(alarmEvent, alarmEvent.getAckData(), config);
            }
        };

        AlarmManager mgr = this.context.getAlarmManager();
        this.alarmFilter = new QualifiedPath.Builder().build();

        if (config.getAlarmsEnabled()) {
            mgr.addListener(alarmFilter, alarmListener);
            log.info("Setup alarm manager - subscribing alarms for streaming - " + Arrays.toString(config.getSource()));
        }
    }

    private void verifySchema(GatewayContext context) {
        try {
            context.getSchemaUpdater().updatePersistentRecords(KafkaSettingsRecord.META);
        } catch (SQLException e) {
            log.error("Error verifying persistent record schemas for KafkaConnect records.", e);
        }
    }

    public void maybeCreateKafkaSettings(GatewayContext context) {
        log.trace("Attempting to create Kafka Settings Record");

        try {
            KafkaSettingsRecord settingsRecord = context.getLocalPersistenceInterface().createNew(KafkaSettingsRecord.META);
            settingsRecord.setId(0L);
            settingsRecord.setBrokerList(Constants.DEFAULT_BROKERS);
            settingsRecord.setTagHistoryTopic(Constants.DEFAULT_TAG_HISTORY_TOPIC);
            settingsRecord.setEnabled(false);
            settingsRecord.setUseStoreAndFwd(false);
            settingsRecord.setUseSSL(false);
            settingsRecord.setAlarmsTopic(Constants.DEFAULT_ALARM_EVENT_TOPIC);
            settingsRecord.setDefaultAlarmPriority();
            settingsRecord.setSource("");
            settingsRecord.setAlarmsEnabled(false);
            settingsRecord.setScriptingEnabled(false);
            settingsRecord.setScriptingTopic(Constants.DEFAULT_SCRIPTING_TOPIC);

            // This doesn't override existing settings, it only sets the above if there is no existing settings

            context.getSchemaUpdater().ensureRecordExists(settingsRecord);
        } catch (Exception e) {
            log.error("Failed to establish Kafka Record exists", e);
        }
        log.trace("Kafka Settings Record Established");
    }

    @Override
    public void startup(LicenseState licenseState) {
        log.info("Kafka module starting...");
    }

    @Override
    public void shutdown() {
        scriptModule.shutDownSinks();
        this.context.getAlarmManager().removeListener(this.alarmFilter, alarmListener);
        BundleUtil.get().removeBundle("kafka");
        log.info("Shutdown complete");
    }

    @Override
    public void initializeScriptManager(ScriptManager manager) {
        super.initializeScriptManager(manager);
        manager.addScriptModule(
                "system.kafka.producer",
                scriptModule,
                new ScriptDocs()
        );
    }

    @Override
    public Object getRPCHandler(ClientReqSession session, String projectName) {
        return scriptModule;
    }

    @Override
    public boolean isFreeModule(){
        return true;
    }

    /** The following methods are used by the status panel. */

    // getMountPathAlias() allows us to use a shorter mount path.
    @Override
    public Optional<String> getMountPathAlias() {
        return Optional.of("kafka");
    }

    // Use this whenever you have mounted resources
    @Override
    public Optional<String> getMountedResourceFolder() {
        return Optional.of("mounted");
    }

    // Define your route handlers here
    @Override
    public void mountRouteHandlers(RouteGroup routes) {
        new KafkaStatusRoutes(this.scriptModule.getDataSinks(), routes).mountRoutes();
    }

    @Override
    public List<? extends INamedTab> getStatusPanels() {
        return Collections.singletonList(HCE_STATUS_PAGE);
    }
}