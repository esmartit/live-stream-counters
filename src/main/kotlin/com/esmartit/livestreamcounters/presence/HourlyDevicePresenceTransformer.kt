package com.esmartit.livestreamcounters.presence

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.WindowStore
import java.time.Instant

class HourlyDevicePresenceTransformer(private val windowStart: Long) :
    Transformer<String, HourlyDevicePresence, KeyValue<String, HourlyDeviceDeltaPresence>> {

    private lateinit var stateStore: WindowStore<String, Position>

    override fun init(context: ProcessorContext) {
        this.stateStore = context.getStateStore(HOURLY_DEVICE_PRESENCE_STORE) as WindowStore<String, Position>
    }

    override fun transform(
        key: String,
        hourlyDevicePresence: HourlyDevicePresence
    ): KeyValue<String, HourlyDeviceDeltaPresence> {
        val timeAndMacAddress = with(hourlyDevicePresence) { "$time:$macAddress" }
        val nowIsh = Instant.now()
        val thenIsh = nowIsh.minusSeconds(windowStart)
        val positionWindow = this.stateStore.fetch(timeAndMacAddress, thenIsh, nowIsh)
        val currentPosition = positionWindow.asSequence().lastOrNull()?.value
        if (currentPosition == null) {
            return streamNewPosition(timeAndMacAddress, hourlyDevicePresence)
        } else if (hourlyDevicePresence.position.value > currentPosition.value) {
            return streamDelta(timeAndMacAddress, hourlyDevicePresence, currentPosition)
        }
        return KeyValue(hourlyDevicePresence.time, HourlyDeviceDeltaPresence(hourlyDevicePresence.time))
    }

    private fun streamDelta(
        timeAndMacAddress: String,
        hourlyDevicePresence: HourlyDevicePresence,
        currentPosition: Position
    ): KeyValue<String, HourlyDeviceDeltaPresence> {
        this.stateStore.put(timeAndMacAddress, hourlyDevicePresence.position)
        return with(hourlyDevicePresence) {
            KeyValue(
                time,
                HourlyDeviceDeltaPresence(time, increment = position, decrement = currentPosition)
            )
        }
    }

    private fun streamNewPosition(
        timeAndMacAddress: String,
        hourlyDevicePresence: HourlyDevicePresence
    ): KeyValue<String, HourlyDeviceDeltaPresence> {
        this.stateStore.put(timeAndMacAddress, hourlyDevicePresence.position)
        return with(hourlyDevicePresence) { KeyValue(time, HourlyDeviceDeltaPresence(time, increment = position)) }
    }

    override fun close() {
    }
}