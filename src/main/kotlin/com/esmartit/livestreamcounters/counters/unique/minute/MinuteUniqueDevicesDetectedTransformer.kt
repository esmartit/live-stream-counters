package com.esmartit.livestreamcounters.counters.unique.minute

import com.esmartit.livestreamcounters.counters.unique.ShouldIncreaseCount
import com.esmartit.livestreamcounters.events.DeviceDetectedEvent
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.WindowStore
import java.time.Instant
import java.time.temporal.ChronoUnit

class MinuteUniqueDevicesDetectedTransformer(private val storeName: String) :
    Transformer<String, DeviceDetectedEvent, KeyValue<String, ShouldIncreaseCount>> {

    private lateinit var stateStore: WindowStore<String, String>

    override fun init(context: ProcessorContext) {
        this.stateStore =
            context.getStateStore(storeName) as WindowStore<String, String>
    }

    override fun transform(
        key: String,
        event: DeviceDetectedEvent
    ): KeyValue<String, ShouldIncreaseCount> {
        val time = Instant.parse(event.device.seenTime).truncatedTo(ChronoUnit.MINUTES)
        val timeAndMacAddress = with(event.device) { "$time:$clientMac" }
        val nowIsh = Instant.now()
        val positionWindow = this.stateStore.fetch(timeAndMacAddress, time, nowIsh)
        val currentPosition = positionWindow.asSequence().lastOrNull()?.value
        return if (currentPosition == null) {
            this.stateStore.put(timeAndMacAddress, "1")
            KeyValue(time.toString(), ShouldIncreaseCount(true))
        } else {
            KeyValue(time.toString(), ShouldIncreaseCount(false))
        }
    }

    override fun close() {
    }
}