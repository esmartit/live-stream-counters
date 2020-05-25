package com.esmartit.livestreamcounters.counters.presence.hourly

import com.esmartit.livestreamcounters.counters.presence.DeviceDeltaPresence
import com.esmartit.livestreamcounters.counters.presence.DevicePresenceStat
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
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.Output
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsStateStoreProperties
import org.springframework.messaging.handler.annotation.SendTo

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
    fun process(input: KStream<String, DeviceWithPresenceEvent>): KStream<String, DevicePresenceStat> {

        return input
            .transform(hourlyDevicePresenceTransformer(), HOURLY_DEVICE_PRESENCE_STORE)
            .filter { _, value -> value.isThereAChange() }
            .groupByKey(Grouped.with(Serdes.String(), JsonSerde(objectMapper, DeviceDeltaPresence::class)))
            .aggregate(
                { DevicePresenceStat() },
                { key, delta, stat -> delta.calculateStats(key, stat) },
                statsKeyStore()
            )
            .toStream()
    }

    private fun hourlyDevicePresenceTransformer() =
        TransformerSupplier { HourlyDevicePresenceTransformer() }

    private fun statsKeyStore() =
        Materialized.`as`<String, DevicePresenceStat, KeyValueStore<Bytes, ByteArray>>("HourlyDevicePresenceStore")
            .withKeySerde(Serdes.String())
            .withValueSerde(JsonSerde(objectMapper, DevicePresenceStat::class))
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