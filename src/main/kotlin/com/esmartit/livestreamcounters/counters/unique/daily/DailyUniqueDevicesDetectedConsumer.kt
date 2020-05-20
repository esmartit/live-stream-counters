package com.esmartit.livestreamcounters.counters.unique.daily


import com.esmartit.livestreamcounters.counters.unique.DeviceCount
import com.esmartit.livestreamcounters.counters.unique.ShouldIncreaseCount
import com.esmartit.livestreamcounters.counters.unique.daily.DailyUniqueDevicesDetectedProcessor.Companion.DAILY_UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT
import com.esmartit.livestreamcounters.counters.unique.daily.DailyUniqueDevicesDetectedProcessor.Companion.POSITION_INPUT
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

internal const val DAILY_UNIQUE_DEVICES_DETECTED_STORE = "daily-unique-devices-detected-store"
internal const val DAILY_UNIQUE_DEVICES_DETECTED_COUNT_STORE = "daily-unique-devices-detected-count-store"
internal const val DAILY_UNIQUE_DEVICES_DETECTED_COUNT_WINDOW_LENGTH = 86_400L

@EnableBinding(DailyUniqueDevicesDetectedProcessor::class)
class DailyUniqueDevicesDetectedConsumer {

    @StreamListener(POSITION_INPUT)
    @SendTo(DAILY_UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT)
    @KafkaStreamsStateStore(
        type = KafkaStreamsStateStoreProperties.StoreType.WINDOW,
        lengthMs = DAILY_UNIQUE_DEVICES_DETECTED_COUNT_WINDOW_LENGTH * 1000,
        retentionMs = DAILY_UNIQUE_DEVICES_DETECTED_COUNT_WINDOW_LENGTH * 1000,
        name = DAILY_UNIQUE_DEVICES_DETECTED_STORE
    )
    fun process(input: KStream<String, DeviceWithPresenceEvent>): KStream<String, DeviceCount> {
        return input
            .mapValues(ValueMapper<DeviceWithPresenceEvent, DeviceDetectedEvent> { it.deviceDetectedEvent })
            .transform(TransformerSupplier { transformer() }, DAILY_UNIQUE_DEVICES_DETECTED_STORE)
            .filter { _, shouldIncreaseCount -> shouldIncreaseCount.value }
            .mapValues(ValueMapper<ShouldIncreaseCount, String> { "1" })
            .groupByKey()
            .count(Materialized.`as`(DAILY_UNIQUE_DEVICES_DETECTED_COUNT_STORE))
            .toStream()
            .mapValues(ValueMapper<Long, DeviceCount> { DeviceCount(it, Instant.now().epochSecond) })
            .peek { key, value -> println("$key-$value") }
    }

    private fun transformer() = DailyUniqueDevicesDetectedTransformer(DAILY_UNIQUE_DEVICES_DETECTED_COUNT_WINDOW_LENGTH)
}

interface DailyUniqueDevicesDetectedProcessor {

    @Input(POSITION_INPUT)
    fun input(): KStream<*, *>

    @Output(DAILY_UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT)
    fun output(): KStream<*, *>

    companion object {
        const val POSITION_INPUT = "daily-unique-devices-detected-count-input"
        const val DAILY_UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT = "daily-unique-devices-detected-count-output"
    }
}
