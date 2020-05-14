package com.esmartit.livestreamcounters.presence

import com.esmartit.livestreamcounters.DeviceDetectedEvent
import com.esmartit.livestreamcounters.presence.Position.IN
import com.esmartit.livestreamcounters.presence.Position.LIMIT
import com.esmartit.livestreamcounters.presence.Position.NO_POSITION
import com.esmartit.livestreamcounters.presence.Position.OUT
import com.esmartit.livestreamcounters.serde.JsonSerde
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TransformerSupplier
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

    @StreamListener(PresenceStreamsProcessor.PRESENCE_INPUT)
    @SendTo(PresenceStreamsProcessor.PRESENCE_OUTPUT)
    @KafkaStreamsStateStore(
        type = KafkaStreamsStateStoreProperties.StoreType.WINDOW,
        lengthMs = HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH * 1000,
        retentionMs = HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH * 1000,
        name = HOURLY_DEVICE_PRESENCE_STORE,
        valueSerde = "com.esmartit.livestreamcounters.serde.PositionSerde"
    )
    fun process(input: KStream<String, DeviceDetectedEvent>): KStream<String, HourlyDevicePresenceStat> {

        return input.mapValues { _, deviceDetectedEvent -> hourlyDevicePresence(deviceDetectedEvent) }
            .transform(hourlyDevicePresenceTransformer(), HOURLY_DEVICE_PRESENCE_STORE)
            .filterNot { _, hourlyDeviceDeltaPresence -> noChangesIn(hourlyDeviceDeltaPresence) }
            .groupByKey(Grouped.with(Serdes.String(), JsonSerde(objectMapper, HourlyDeviceDeltaPresence::class)))
            .aggregate({ HourlyDevicePresenceStat() }, ::calculateStats, statsKeyStore())
            .toStream()
            .peek { key, value -> println(value) }
    }

    private fun hourlyDevicePresence(deviceDetectedEvent: DeviceDetectedEvent): HourlyDevicePresence {
        val seenTime = Instant.parse(deviceDetectedEvent.device.seenTime).truncatedTo(ChronoUnit.HOURS)
        val position = presence(deviceDetectedEvent.device.rssi)
        return HourlyDevicePresence(deviceDetectedEvent.device.clientMac, position, seenTime.toString())
    }

    private fun presence(power: Int) = when {
        power >= -35 -> IN
        power >= -45 -> LIMIT
        else -> OUT
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

interface PresenceStreamsProcessor {

    @Input(PRESENCE_INPUT)
    fun input(): KStream<*, *>

    @Output(PRESENCE_OUTPUT)
    fun output(): KStream<*, *>

    companion object {
        const val PRESENCE_INPUT = "presence-input"
        const val PRESENCE_OUTPUT = "presence-output"
    }
}
