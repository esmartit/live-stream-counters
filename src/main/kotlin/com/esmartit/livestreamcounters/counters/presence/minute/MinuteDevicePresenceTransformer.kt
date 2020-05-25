package com.esmartit.livestreamcounters.counters.presence.minute


import com.esmartit.livestreamcounters.counters.presence.DeviceDeltaPresence
import com.esmartit.livestreamcounters.counters.presence.DevicePresence
import com.esmartit.livestreamcounters.events.DeviceWithPresenceEvent
import com.esmartit.livestreamcounters.sensor.Position
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.WindowStore
import java.time.Instant
import java.time.temporal.ChronoUnit

class MinuteDevicePresenceTransformer :
    Transformer<String, DeviceWithPresenceEvent, KeyValue<String, DeviceDeltaPresence>> {

    private lateinit var stateStore: WindowStore<String, Position>

    override fun init(context: ProcessorContext) {
        this.stateStore = context.getStateStore(MINUTE_DEVICE_PRESENCE_STORE) as WindowStore<String, Position>
    }

    override fun transform(
        key: String,
        withPosition: DeviceWithPresenceEvent
    ): KeyValue<String, DeviceDeltaPresence> {

        val deviceDetectedEvent = withPosition.deviceDetectedEvent
        val position = withPosition.position
        val macAddress = deviceDetectedEvent.device.clientMac

        val seenTime = Instant.parse(deviceDetectedEvent.device.seenTime).truncatedTo(ChronoUnit.MINUTES)

        val minuteDevicePresence = DevicePresence(macAddress, position, seenTime.toString())
        val timeAndMacAddress = "$seenTime:$macAddress"
        val nowIsh = Instant.now()
        val positionWindow = this.stateStore.fetch(timeAndMacAddress, seenTime, nowIsh)
        val currentPosition = positionWindow.asSequence().lastOrNull()?.value

        if (currentPosition == null) {
            return streamNewPosition(timeAndMacAddress, minuteDevicePresence)
        } else if (minuteDevicePresence.position.value > currentPosition.value) {
            return streamDelta(timeAndMacAddress, minuteDevicePresence, currentPosition)
        }
        return KeyValue(minuteDevicePresence.time, DeviceDeltaPresence(minuteDevicePresence.time))
    }

    private fun streamDelta(
        timeAndMacAddress: String,
        devicePresence: DevicePresence,
        currentPosition: Position
    ): KeyValue<String, DeviceDeltaPresence> {
        this.stateStore.put(timeAndMacAddress, devicePresence.position)
        return with(devicePresence) {
            KeyValue(time, DeviceDeltaPresence(time, increment = position, decrement = currentPosition))
        }
    }

    private fun streamNewPosition(
        timeAndMacAddress: String,
        devicePresence: DevicePresence
    ): KeyValue<String, DeviceDeltaPresence> {
        this.stateStore.put(timeAndMacAddress, devicePresence.position)
        return with(devicePresence) {
            KeyValue(time, DeviceDeltaPresence(time, increment = position))
        }
    }

    override fun close() {
    }
}