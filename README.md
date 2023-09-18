# ignition-kafka
Kafka Producer Module for Ignition Scada.  Can be used to stream tag history, alarms, and a scripting data to seperate topics.  From there the data can be used by other consumers or sunk into a central database.

## Usage
With the module installed and connnected to a kafka broker, tag history can be sent in either of the following ways:
### Tag History
- setting the history provider on a tag to be ```kafka-tag-history```
- use ```kafka-tag-history``` on a tag splitter sending data to a DB of choice and kafka
- use the ignition provided function [system.tag.storeTagHistory](https://docs.inductiveautomation.com/display/DOC81/system.tag.storeTagHistory) with the ```historyprovider``` set to ```ignition-tag-history```

### Alarms
With alarms enabled on the settings page, an configured alarm that meets severity, source, display path, and source paths requirements will be transmitted.  So outside of the settings page, simply configure alarm and the ```alarmListener``` will pick up any alarm traffic and send it to the specified kafka topic.

### Scripting
By using the module provided function ```system.kafka.producer.sendScriptingData``` by providing a ```key``` and and a ```value```.  This function allows for various data to be sent when the pipeline and data content need to be different than what can fit into tag history and alarms.

## Examples
### Tag History
![image](https://github.com/Freeno83/ignition-kafka/assets/60759127/5ee43398-59c1-4f05-8905-f5cbcc9e5780)

By changing the tag value, the value is automatically transmitted:

![image](https://github.com/Freeno83/ignition-kafka/assets/60759127/c2b6d679-9596-456b-97dc-2786c8ebe382)
