package com.esmartit.livestreamcounters.uniquedevicesdetected

import com.esmartit.livestreamcounters.DeviceDetectedEvent
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.kstream.ValueMapper
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore
import org.springframework.messaging.handler.annotation.SendTo
import java.time.Instant

internal const val UNIQUE_DEVICES_DETECTED_STORE = "unique-devices-detected-store"
internal const val UNIQUE_DEVICES_DETECTED_COUNT = "unique-devices-detected-count"
internal const val UNIQUE_DEVICES_DETECTED_COUNT_STORE = "unique-devices-detected-count-store"

@EnableBinding(KafkaStreamsProcessor::class)
class UniqueDevicesDetectedConsumer {

    @StreamListener("input")
    @SendTo("output")
    @KafkaStreamsStateStore(name = UNIQUE_DEVICES_DETECTED_STORE)
    fun process(input: KStream<String, DeviceDetectedEvent>): KStream<String, UniqueDevicesDetectedCount> {
        return input
            .transform(TransformerSupplier { UniqueDevicesDetectedTransformer() }, UNIQUE_DEVICES_DETECTED_STORE)
            .filter { _, shouldIncreaseCount -> shouldIncreaseCount.value }
            .mapValues(ValueMapper<ShouldIncreaseCount, String> { "1" })
            .groupByKey()
            .count(Materialized.`as`(UNIQUE_DEVICES_DETECTED_COUNT_STORE))
            .toStream()
            .mapValues(ValueMapper { UniqueDevicesDetectedCount(it, Instant.now().epochSecond) })
    }
}
