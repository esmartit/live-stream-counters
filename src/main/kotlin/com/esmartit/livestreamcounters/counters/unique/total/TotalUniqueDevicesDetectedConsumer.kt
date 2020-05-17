package com.esmartit.livestreamcounters.counters.unique.total

import com.esmartit.livestreamcounters.counters.unique.DeviceCount
import com.esmartit.livestreamcounters.counters.unique.ShouldIncreaseCount
import com.esmartit.livestreamcounters.counters.unique.total.TotalUniqueDevicesDetectedProcessor.Companion.POSITION_INPUT
import com.esmartit.livestreamcounters.counters.unique.total.TotalUniqueDevicesDetectedProcessor.Companion.UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT
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
import org.springframework.messaging.handler.annotation.SendTo
import java.time.Instant

internal const val UNIQUE_DEVICES_DETECTED_STORE = "unique-devices-detected-store"
internal const val UNIQUE_DEVICES_DETECTED_COUNT = "unique-devices-detected-count"
internal const val UNIQUE_DEVICES_DETECTED_COUNT_STORE = "unique-devices-detected-count-store"

@EnableBinding(TotalUniqueDevicesDetectedProcessor::class)
class TotalUniqueDevicesDetectedConsumer {

    @StreamListener(POSITION_INPUT)
    @SendTo(UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT)
    @KafkaStreamsStateStore(name = UNIQUE_DEVICES_DETECTED_STORE)
    fun process(input: KStream<String, DeviceWithPresenceEvent>): KStream<String, DeviceCount> {
        return input
            .mapValues(ValueMapper<DeviceWithPresenceEvent, DeviceDetectedEvent> { it.deviceDetectedEvent })
            .transform(TransformerSupplier { TotalUniqueDevicesDetectedTransformer() }, UNIQUE_DEVICES_DETECTED_STORE)
            .filter { _, shouldIncreaseCount -> shouldIncreaseCount.value }
            .mapValues(ValueMapper<ShouldIncreaseCount, String> { "1" })
            .groupByKey()
            .count(Materialized.`as`(UNIQUE_DEVICES_DETECTED_COUNT_STORE))
            .toStream()
            .mapValues(ValueMapper<Long, DeviceCount> { DeviceCount(it, Instant.now().epochSecond) })
            .peek { key, value -> println("$key-$value") }
    }
}

interface TotalUniqueDevicesDetectedProcessor {

    @Input(POSITION_INPUT)
    fun input(): KStream<*, *>

    @Output(UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT)
    fun output(): KStream<*, *>

    companion object {
        const val POSITION_INPUT = "unique-devices-detected-count-input"
        const val UNIQUE_DEVICES_DETECTED_COUNT_OUTPUT = "unique-devices-detected-count-output"
    }
}
