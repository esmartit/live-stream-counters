package com.esmartit.livestreamcounters.counters.presence.hourly

import com.esmartit.livestreamcounters.counters.presence.hourly.PresenceStreamsProcessor.Companion.HOURLY_DEVICE_PRESENCE_OUTPUT
import com.esmartit.livestreamcounters.counters.presence.hourly.PresenceStreamsProcessor.Companion.PRESENCE_INPUT
import com.esmartit.livestreamcounters.events.DeviceWithPresenceEvent
import com.esmartit.livestreamcounters.serde.JsonSerde
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.kstream.ValueMapper
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
internal const val HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH = 3_600L

@EnableBinding(PresenceStreamsProcessor::class)
class HourlyDevicePresenceConsumer(private val objectMapper: ObjectMapper) {

    @StreamListener(PRESENCE_INPUT)
    @SendTo(HOURLY_DEVICE_PRESENCE_OUTPUT)
    @KafkaStreamsStateStore(
        type = KafkaStreamsStateStoreProperties.StoreType.WINDOW,
        lengthMs = HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH * 1000,
        retentionMs = HOURLY_DEVICE_PRESENCE_WINDOW_LENGTH * 1000,
        name = HOURLY_DEVICE_PRESENCE_STORE,
        valueSerde = "com.esmartit.livestreamcounters.serde.PositionSerde"
    )
    fun process(input: KStream<String, DeviceWithPresenceEvent>): KStream<String, HourlyDevicePresenceStat> {

        return input
            .mapValues(ValueMapper<DeviceWithPresenceEvent, HourlyDevicePresence> { hourlyDevicePresence(it) })
            .transform(hourlyDevicePresenceTransformer(), HOURLY_DEVICE_PRESENCE_STORE)
            .filter { _, value -> value.isThereAChange() }
            .groupByKey(Grouped.with(Serdes.String(), JsonSerde(objectMapper, HourlyDeviceDeltaPresence::class)))
            .aggregate(
                { HourlyDevicePresenceStat() },
                { key, delta, stat -> delta.calculateStats(key, stat) },
                statsKeyStore()
            )
            .toStream()
            .peek { key, value -> println("$key-$value") }
    }

    private fun hourlyDevicePresence(
        withPosition: DeviceWithPresenceEvent
    ): HourlyDevicePresence {
        val deviceDetectedEvent = withPosition.deviceDetectedEvent
        val seenTime = Instant.parse(deviceDetectedEvent.device.seenTime).truncatedTo(ChronoUnit.HOURS)
        val position = withPosition.position
        return HourlyDevicePresence(deviceDetectedEvent.device.clientMac, position, seenTime.toString())
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

    @Output(HOURLY_DEVICE_PRESENCE_OUTPUT)
    fun output(): KStream<*, *>

    companion object {
        const val PRESENCE_INPUT = "hourly-presence-count-input"
        const val HOURLY_DEVICE_PRESENCE_OUTPUT = "hourly-presence-count-output"
    }
}