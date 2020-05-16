package com.esmartit.livestreamcounters.presence

import com.esmartit.livestreamcounters.DeviceDetectedEvent
import com.esmartit.livestreamcounters.presence.Position.IN
import com.esmartit.livestreamcounters.presence.Position.LIMIT
import com.esmartit.livestreamcounters.presence.Position.NO_POSITION
import com.esmartit.livestreamcounters.presence.Position.OUT
import com.esmartit.livestreamcounters.presence.PresenceStreamsProcessor.Companion.PRESENCE_INPUT
import com.esmartit.livestreamcounters.presence.PresenceStreamsProcessor.Companion.SENSOR_SETTINGS_INPUT
import com.esmartit.livestreamcounters.serde.JsonSerde
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KeyValueMapper
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.kstream.ValueJoiner
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.Output
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsStateStoreProperties
import org.springframework.messaging.handler.annotation.SendTo
import java.time.Instant
import java.time.temporal.ChronoUnit

internal const val HOURLY_DEVICE_PRESENCE_STORE = "hourly-device-presence-store"
internal const val HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH = 7_200L

@EnableBinding(PresenceStreamsProcessor::class)
class HourlyDevicePresenceConsumer(private val objectMapper: ObjectMapper) {

    @StreamListener
    @SendTo(PresenceStreamsProcessor.PRESENCE_OUTPUT)
    @KafkaStreamsStateStore(
        type = KafkaStreamsStateStoreProperties.StoreType.WINDOW,
        lengthMs = HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH * 1000,
        retentionMs = HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH * 1000,
        name = HOURLY_DEVICE_PRESENCE_STORE,
        valueSerde = "com.esmartit.livestreamcounters.serde.PositionSerde"
    )
    fun process(
        @Input(PRESENCE_INPUT) input: KStream<String, DeviceDetectedEvent>,
        @Input(SENSOR_SETTINGS_INPUT) sensorSettings: GlobalKTable<String, SensorSetting>
    ): KStream<String, HourlyDevicePresenceStat> {

        return input
            .leftJoin(sensorSettings,
                KeyValueMapper<String, DeviceDetectedEvent, String> { _, event -> event.sensorName },
                ValueJoiner<DeviceDetectedEvent, SensorSetting, HourlyDevicePresence> { event, setting ->
                    hourlyDevicePresence(event, setting)
                })
            .transform(hourlyDevicePresenceTransformer(), HOURLY_DEVICE_PRESENCE_STORE)
            .filterNot { _, hourlyDeviceDeltaPresence -> noChangesIn(hourlyDeviceDeltaPresence) }
            .groupByKey(Grouped.with(Serdes.String(), JsonSerde(objectMapper, HourlyDeviceDeltaPresence::class)))
            .aggregate({ HourlyDevicePresenceStat() }, ::calculateStats, statsKeyStore())
            .toStream()
            .peek { _, value -> println(value) }
    }

    private fun hourlyDevicePresence(
        deviceDetectedEvent: DeviceDetectedEvent,
        setting: SensorSetting?
    ): HourlyDevicePresence {
        val seenTime = Instant.parse(deviceDetectedEvent.device.seenTime).truncatedTo(ChronoUnit.HOURS)
        val sensorSetting = setting ?: SensorSetting("default")
        val position = sensorSetting.presence(deviceDetectedEvent.device.rssi)
        return HourlyDevicePresence(deviceDetectedEvent.device.clientMac, position, seenTime.toString())
    }

    private fun noChangesIn(hourlyDeviceDeltaPresence: HourlyDeviceDeltaPresence) =
        hourlyDeviceDeltaPresence.increment == NO_POSITION && hourlyDeviceDeltaPresence.decrement == NO_POSITION

    private fun calculateStats(time: String, delta: HourlyDeviceDeltaPresence, stat: HourlyDevicePresenceStat) =
        decrementDelta(time, delta, incrementDelta(time, delta, stat))

    private fun incrementDelta(time: String, delta: HourlyDeviceDeltaPresence, stat: HourlyDevicePresenceStat) =
        when (delta.increment) {
            IN -> stat.copy(time = time, inCount = stat.inCount + 1)
            LIMIT -> stat.copy(time = time, limitCount = stat.limitCount + 1)
            OUT -> stat.copy(time = time, outCount = stat.outCount + 1)
            NO_POSITION -> stat
        }

    private fun decrementDelta(time: String, delta: HourlyDeviceDeltaPresence, stat: HourlyDevicePresenceStat) =
        when (delta.decrement) {
            IN -> stat.copy(time = time, inCount = stat.inCount - 1)
            LIMIT -> stat.copy(time = time, limitCount = stat.limitCount - 1)
            OUT -> stat.copy(time = time, outCount = stat.outCount - 1)
            NO_POSITION -> stat
        }

    private fun hourlyDevicePresenceTransformer() =
        TransformerSupplier { HourlyDevicePresenceTransformer(HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH) }

    private fun statsKeyStore() =
        Materialized.`as`<String, HourlyDevicePresenceStat, KeyValueStore<Bytes, ByteArray>>("HourlyDevicePresenceStore")
            .withKeySerde(Serdes.String())
            .withValueSerde(JsonSerde(objectMapper, HourlyDevicePresenceStat::class))
}

data class SensorSetting(
    val id: String,
    val spot: String = "default",
    val sensorId: String = "default",
    val location: String = "default",
    val inEdge: Int = -35,
    val limitEdge: Int = -45,
    val outEdge: Int = -300
) {
    fun presence(power: Int) = when {
        power >= inEdge -> IN
        power >= limitEdge -> LIMIT
        power >= outEdge -> OUT
        else -> NO_POSITION
    }
}

interface PresenceStreamsProcessor {

    @Input(PRESENCE_INPUT)
    fun input(): KStream<*, *>

    @Output(PRESENCE_OUTPUT)
    fun output(): KStream<*, *>

    @Input(SENSOR_SETTINGS_INPUT)
    fun inputTable(): GlobalKTable<*, *>

    companion object {
        const val PRESENCE_INPUT = "presence-input"
        const val PRESENCE_OUTPUT = "presence-output"
        const val SENSOR_SETTINGS_INPUT = "sensor-settings-input"
    }
}
