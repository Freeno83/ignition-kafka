import React, {Component} from 'react';
import {connect} from 'react-redux';
import {pollWaitAck} from 'ignition-lib';
import {getConnectionsStatus} from './model';
import {BlankState, Gauge, ItemTable, Loading} from 'ignition-react';

const BLANK_STATE = {
    image: <img src="/main/res/alarm-notification/img/blank_alarms.png" alt=""/>,
    heading: 'There are no sinks defined.',
    body: 'Kafka sinks allow you to stream data from Ignition to Kafka.',

    links: [<a className="primary button"
               target="_blank"
               href="https://kafka.apache.org/">Learn More</a>
    ]
};

class GaugeSmall extends Component{
    constructor(props) {
        super(props);
        this.label = props.label
        this.value = props.value
        this.units = props.units
    }

    render(){
        const label = this.label
        const value = this.value
        const units = this.units
        return(<div className="gauge-container" style={{padding: 0, margin: 0, marginBottom: "2rem"}}>
                    <div className="gauge" style={{padding: "0.1rem"}}>
                        <div className="title">
                            <div>{label}</div>
                        </div>
                        <div className="data" style={{margin: "0px", position: "relative", zIndex: 1}}>
                            {value}
                            <span className="unit">
                                {units}
                            </span>
                        </div>
                    </div>
                </div>
        );
    }
}

class ConnectOverview extends Component {
    constructor(props) {
        super(props);
    }

    componentWillMount() {
        const {dispatch} = this.props;
        // // refresh the connection status every 5 seconds, but don't start a new request until the last one has returned
        this.cancelPoll = pollWaitAck(dispatch, getConnectionsStatus, 5000);
    }

    componentWillUnmount() {
        if (this.cancelPoll) {
            this.cancelPoll();
        }
    }

    render() {
        const {connections, connectionsError} = this.props;

        if (connections != null){
            const HEADERS = [
                { header: 'Use SSL', weight: 2 },
                { header: 'Tag History Topic', weight: 2},
                { header: 'Alarms Topic', weight: 2},
                { header: 'Scripting Topic', weight: 2},
            ];
            const alarmHeaders = [
                { header: 'Minimum Priority', weight: 1},
                { header: 'Source', weight: 1 },
                { header: 'Display Path', weight: 1 },
                { header: 'Source Path', weight: 2 }
            ];
            const statHeaders = [
                { header: "Source", weight: 1 },
                { header: "Messages Sent", weight: 1 },
                { header: "Failed to Send", weight: 1 },
                { header: "Last Sent", weight: 1 },
                { header: "Up Time (days)", weight: 1 },
                { header: "Start Time", weight: 1 }
            ];
            const brokerHeaders = [
                { header: "Brokers", weight: 2 }
            ];

            const connectionCount = connections.count;
            const enabled = connections.Enabled ? 'YES' : 'NO';
            const storeAndFwd = connections.UseStoreAndForward ? 'YES' : 'NO';
            const alarmsEnabled = connections.AlarmsEnabled ? 'YES' : 'NO';
            const scriptingEnabled = connections.ScriptingEnabled ? 'YES': 'NO';

            if (connectionCount > 0){
                const connectionList = connections.connections;
                let settings = [], alarmSettings = [], brokers = [];
                if (connectionList != null){
                    settings = connectionList.map((connection) => {
                        return [
                            connection.isSSL.toString(),
                            connection.TagHistoryTopic,
                            connection.AlarmsTopic,
                            connection.ScriptingTopic
                        ];
                    });
                    alarmSettings = connectionList.map((connection) => {
                        return [
                            connection.MinimumPriority,
                            connection.Source == null ? 'none' : connection.Source,
                            connection.DispPath == null ? 'none' : connection.DispPath,
                            connection.SrcPath == null ? 'none' : connection.SrcPath
                        ];
                    });
                    brokers = connectionList[0].Brokers.split(',').map(val => [val])
                }

                const sinkList = connections.sinks;
                let stats = [];
                if (sinkList != null) {
                    stats = mapMany(sinkList, function(c) {
                        return c.stats.map(function(s) {
                            return [
                                s.Source,
                                s.MessageCount.toString(),
                                s.FailedCount.toString(),
                                s.LastMessageTime,
                                s.LifeSpan.toString(),
                                s.Started
                            ]
                        })
                    });
                }

                return (<div>
                    <div className="row">
                        <div className="small-12 columns">
                            <div className="page-heading">
                                <div className="quick-links">
                                    <a href="/web/config/kafka.kafka">Configure</a>
                                </div>
                                <h6>Systems</h6>
                                <h1>Performance</h1>
                            </div>
                        </div>
                    </div>
                    <div className="row">
                        <div className="small-12 medium-5 large-3 columns">
                            <GaugeSmall label="Kafka Enabled" value={enabled}/>
                        </div>

                        <div className="small-12 medium-5 large-3 columns">
                            <GaugeSmall label="Store & Forward Enabled" value={storeAndFwd}/>
                        </div>

                        <div className="small-12 medium-5 large-3 columns">
                            <GaugeSmall label="Alarms Enabled" value={alarmsEnabled}/>
                        </div>

                        <div className="small-12 medium-5 large-3 columns">
                            <GaugeSmall label="Scripting Enabled" value={scriptingEnabled}/>
                        </div>
                    </div>
                    <div className="row">
                      <h5>Connections</h5>
                        <div className="small-12 columns">
                            <ItemTable headers={ brokerHeaders } items={ brokers } errorMessage={connectionsError}/>
                        </div>
                    </div>
                    <div className="row">
                      <h5>Kafka Settings</h5>
                        <div className="small-12 columns">
                            <ItemTable headers={ HEADERS } items={ settings } errorMessage={connectionsError}/>
                        </div>
                    </div>
                    <div className="row">
                      <h5>Alarm Filters</h5>
                        <div className="small-12 columns">
                            <ItemTable headers={ alarmHeaders } items={ alarmSettings } errorMessage={connectionsError}/>
                        </div>
                    </div>
                    <div className="row">
                      <h5>Message Stats</h5>
                        <div className="small-12 columns">
                            <ItemTable headers={ statHeaders } items={ stats } errorMessage={connectionsError}/>
                        </div>
                    </div>
                </div>);
            } else {
                return (<div><BlankState { ...BLANK_STATE } /></div>);
            }

        }else {
            return (<div><Loading /></div>);
        }
    }
}

function selector(state) {
    return {
        connections: state.getConnections,
        connectionsError: state.getConnectionsError,
    }
}

function mapMany (arr, mapper) {
    return arr.reduce(function (prev, curr, i) {
        return prev.concat(mapper(curr));
    },[]);
}

export default connect(selector)(ConnectOverview);