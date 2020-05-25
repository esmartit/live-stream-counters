package com.esmartit.livestreamcounters.counters.presence.minute

import com.esmartit.livestreamcounters.counters.presence.DeviceDeltaPresence
import com.esmartit.livestreamcounters.counters.presence.HourlyDevicePresenceStat
import com.esmartit.livestreamcounters.counters.presence.minute.MinutePresenceStreamsProcessor.Companion.MINUTE_DEVICE_PRESENCE_OUTPUT
import com.esmartit.livestreamcounters.counters.presence.minute.MinutePresenceStreamsProcessor.Companion.PRESENCE_INPUT
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

internal const val MINUTE_DEVICE_PRESENCE_STORE = "minute-device-presence-store"
internal const val MINUTE_DEVICE_PRESENCE_WINDOW_LENGTH = 3_600L
private const val MINUTE_DEVICE_PRESENCE_STORE_COUNT = "MinuteDevicePresenceStore"

@EnableBinding(MinutePresenceStreamsProcessor::class)
class MinuteDevicePresenceConsumer(private val objectMapper: ObjectMapper) {

    @StreamListener(PRESENCE_INPUT)
    @SendTo(MINUTE_DEVICE_PRESENCE_OUTPUT)
    @KafkaStreamsStateStore(
        type = KafkaStreamsStateStoreProperties.StoreType.WINDOW,
        lengthMs = MINUTE_DEVICE_PRESENCE_WINDOW_LENGTH * 1000,
        retentionMs = MINUTE_DEVICE_PRESENCE_WINDOW_LENGTH * 1000,
        name = MINUTE_DEVICE_PRESENCE_STORE,
        valueSerde = "com.esmartit.livestreamcounters.serde.PositionSerde"
    )
    fun process(input: KStream<String, DeviceWithPresenceEvent>): KStream<String, HourlyDevicePresenceStat> {

        return input
            .transform(minuteDevicePresenceTransformer(), MINUTE_DEVICE_PRESENCE_STORE)
            .filter { _, value -> value.isThereAChange() }
            .groupByKey(Grouped.with(Serdes.String(), JsonSerde(objectMapper, DeviceDeltaPresence::class)))
            .aggregate(
                { HourlyDevicePresenceStat() },
                { key, delta, stat -> delta.calculateStats(key, stat) },
                statsKeyStore()
            )
            .toStream()
            .peek { key, value -> println("$key-$value") }
    }

    private fun minuteDevicePresenceTransformer() =
        TransformerSupplier { MinuteDevicePresenceTransformer() }

    private fun statsKeyStore() =
        Materialized.`as`<String, HourlyDevicePresenceStat, KeyValueStore<Bytes, ByteArray>>(
            MINUTE_DEVICE_PRESENCE_STORE_COUNT
        )
            .withKeySerde(Serdes.String())
            .withValueSerde(JsonSerde(objectMapper, HourlyDevicePresenceStat::class))
}

interface MinutePresenceStreamsProcessor {

    @Input(PRESENCE_INPUT)
    fun input(): KStream<*, *>

    @Output(MINUTE_DEVICE_PRESENCE_OUTPUT)
    fun output(): KStream<*, *>

    companion object {
        const val PRESENCE_INPUT = "minute-presence-count-input"
        const val MINUTE_DEVICE_PRESENCE_OUTPUT = "minute-presence-count-output"
    }
}