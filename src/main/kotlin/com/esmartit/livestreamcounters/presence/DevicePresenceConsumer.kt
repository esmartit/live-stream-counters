package com.esmartit.livestreamcounters.presence

import com.esmartit.livestreamcounters.presence.PositionStreamsProcessor.Companion.POSITION_OUTPUT
import com.esmartit.livestreamcounters.presence.PositionStreamsProcessor.Companion.PRESENCE_INPUT
import com.esmartit.livestreamcounters.presence.PositionStreamsProcessor.Companion.SENSOR_SETTINGS_INPUT
import com.esmartit.livestreamcounters.events.DeviceDetectedEvent
import com.esmartit.livestreamcounters.events.DeviceWithPresenceEvent
import com.esmartit.livestreamcounters.sensor.SensorSetting
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KeyValueMapper
import org.apache.kafka.streams.kstream.ValueJoiner
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor
import org.springframework.messaging.handler.annotation.SendTo

@EnableBinding(PositionStreamsProcessor::class)
class DevicePresenceConsumer {

    @StreamListener
    @SendTo(POSITION_OUTPUT)
    fun process(
        @Input(PRESENCE_INPUT) input: KStream<String, DeviceDetectedEvent>,
        @Input(SENSOR_SETTINGS_INPUT) sensorSettings: GlobalKTable<String, SensorSetting>
    ): KStream<String, DeviceWithPresenceEvent> {
        return input
            .leftJoin(sensorSettings,
                KeyValueMapper<String, DeviceDetectedEvent, String> { _, event -> event.sensorName },
                ValueJoiner<DeviceDetectedEvent, SensorSetting, DeviceWithPresenceEvent> { event, setting ->
                    deviceWithPosition(event, setting ?: SensorSetting("default"))
                })
            .filter { _, event -> event.isWithinRange() }
    }

    private fun deviceWithPosition(
        deviceDetectedEvent: DeviceDetectedEvent,
        setting: SensorSetting
    ): DeviceWithPresenceEvent {
        return DeviceWithPresenceEvent(deviceDetectedEvent, setting.presence(deviceDetectedEvent.device.rssi))
    }
}

interface PositionStreamsProcessor : KafkaStreamsProcessor {

    @Input(SENSOR_SETTINGS_INPUT)
    fun inputTable(): GlobalKTable<*, *>

    companion object {
        const val PRESENCE_INPUT = "input"
        const val POSITION_OUTPUT = "output"
        const val SENSOR_SETTINGS_INPUT = "sensor-settings-input"
    }
}
