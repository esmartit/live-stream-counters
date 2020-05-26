package com.esmartit.livestreamcounters.counters.unique.minute


import com.esmartit.livestreamcounters.counters.unique.DeviceCount
import com.esmartit.livestreamcounters.counters.unique.ShouldIncreaseCount
import com.esmartit.livestreamcounters.counters.unique.minute.MinuteUniqueDevicesDetectedProcessor.Companion.MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT
import com.esmartit.livestreamcounters.counters.unique.minute.MinuteUniqueDevicesDetectedProcessor.Companion.POSITION_INPUT
import com.esmartit.livestreamcounters.events.DeviceDetectedEvent
import com.esmartit.livestreamcounters.events.DeviceWithPresenceEvent
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.kstream.ValueMapper
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.Output
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsStateStoreProperties
import org.springframework.messaging.handler.annotation.SendTo
import java.time.Instant

private const val MINUTE_UNIQUE_DEVICES_DETECTED_STORE = "minute-unique-devices-detected-store"
private const val MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_STORE = "minute-unique-devices-detected-count-store"
private const val MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_WINDOW_LENGTH = 36_600L

@EnableBinding(MinuteUniqueDevicesDetectedProcessor::class)
class MinuteUniqueDevicesDetectedConsumer {

    @StreamListener(POSITION_INPUT)
    @SendTo(MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT)
    @KafkaStreamsStateStore(
        type = KafkaStreamsStateStoreProperties.StoreType.WINDOW,
        lengthMs = MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_WINDOW_LENGTH * 1000,
        retentionMs = MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_WINDOW_LENGTH * 1000,
        name = MINUTE_UNIQUE_DEVICES_DETECTED_STORE
    )
    fun process(input: KStream<String, DeviceWithPresenceEvent>): KStream<String, DeviceCount> {
        return input
            .mapValues(ValueMapper<DeviceWithPresenceEvent, DeviceDetectedEvent> { it.deviceDetectedEvent })
            .transform(TransformerSupplier { transformer() }, MINUTE_UNIQUE_DEVICES_DETECTED_STORE)
            .filter { _, shouldIncreaseCount -> shouldIncreaseCount.value }
            .mapValues(ValueMapper<ShouldIncreaseCount, String> { "1" })
            .groupByKey()
            .count(Materialized.`as`(MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_STORE))
            .toStream()
            .mapValues(ValueMapper<Long, DeviceCount> { DeviceCount(it, Instant.now().epochSecond) })
            .peek { key, value -> println("${Instant.now()}----MinuteCount:::>>> $key-$value") }
    }

    private fun transformer() = MinuteUniqueDevicesDetectedTransformer(MINUTE_UNIQUE_DEVICES_DETECTED_STORE)
}

interface MinuteUniqueDevicesDetectedProcessor {

    @Input(POSITION_INPUT)
    fun input(): KStream<*, *>

    @Output(MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT)
    fun output(): KStream<*, *>

    companion object {
        const val POSITION_INPUT = "minute-unique-devices-detected-count-input"
        const val MINUTE_UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT = "minute-unique-devices-detected-count-output"
    }
}
