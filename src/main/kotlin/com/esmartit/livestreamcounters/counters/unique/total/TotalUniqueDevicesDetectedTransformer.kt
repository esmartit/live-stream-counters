package com.esmartit.livestreamcounters.counters.unique.total

import com.esmartit.livestreamcounters.events.DeviceDetectedEvent
import com.esmartit.livestreamcounters.counters.unique.ShouldIncreaseCount
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class TotalUniqueDevicesDetectedTransformer :
    Transformer<String, DeviceDetectedEvent, KeyValue<String, ShouldIncreaseCount>> {

    private lateinit var stateStore: KeyValueStore<String, String>

    override fun init(context: ProcessorContext) {
        this.stateStore = context.getStateStore(UNIQUE_DEVICES_DETECTED_STORE) as KeyValueStore<String, String>
    }

    override fun transform(key: String, value: DeviceDetectedEvent): KeyValue<String, ShouldIncreaseCount> {
        val storedRecord = this.stateStore[key]
        if (storedRecord == null) {
            this.stateStore.put(key, "1")
            return KeyValue(
                UNIQUE_DEVICES_DETECTED_COUNT,
                ShouldIncreaseCount(true)
            )
        }
        return KeyValue(
            UNIQUE_DEVICES_DETECTED_COUNT,
            ShouldIncreaseCount(false)
        )
    }

    override fun close() {
    }
}