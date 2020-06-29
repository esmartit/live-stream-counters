package com.esmartit.livestreamcounters.counters.registered.total

import com.esmartit.livestreamcounters.counters.unique.DeviceCount
import com.esmartit.livestreamcounters.counters.unique.ShouldIncreaseCount
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

internal const val UNIQUE_DEVICES_REGISTERED_STORE = "unique-devices-registered-store"
internal const val UNIQUE_DEVICES_REGISTERED_COUNT = "unique-devices-registered-count"
internal const val UNIQUE_DEVICES_REGISTERED_COUNT_STORE = "unique-devices-registered-count-store"

@EnableBinding(TotalUniqueDevicesRegisteredProcessor::class)
class TotalUniqueDevicesRegisteredConsumer {

    @StreamListener(TotalUniqueDevicesRegisteredProcessor.REGISTERED_INPUT)
    @SendTo(TotalUniqueDevicesRegisteredProcessor.REGISTERED_COUNT_OUTPUT)
    @KafkaStreamsStateStore(name = UNIQUE_DEVICES_REGISTERED_STORE)
    fun process(input: KStream<String, SignUpEvent>): KStream<String, DeviceCount> {
        return input
            .transform(
                TransformerSupplier { TotalUniqueDevicesRegisteredTransformer() },
                UNIQUE_DEVICES_REGISTERED_STORE
            )
            .filter { _, shouldIncreaseCount -> shouldIncreaseCount.value }
            .mapValues(ValueMapper<ShouldIncreaseCount, String> { "1" })
            .groupByKey()
            .count(Materialized.`as`(UNIQUE_DEVICES_REGISTERED_COUNT_STORE))
            .toStream()
            .mapValues(ValueMapper<Long, DeviceCount> { DeviceCount(it, Instant.now().epochSecond) })
            .peek { key, value -> println("TotalUniqueDevicesRegisteredConsumer.process $key - $value") }
    }
}

interface TotalUniqueDevicesRegisteredProcessor {

    @Input(REGISTERED_INPUT)
    fun input(): KStream<*, *>

    @Output(REGISTERED_COUNT_OUTPUT)
    fun output(): KStream<*, *>

    companion object {
        const val REGISTERED_INPUT = "registered-unique-devices-input"
        const val REGISTERED_COUNT_OUTPUT = "registered-unique-devices-count-output"
    }
}

data class SignUpEvent(val clientMac: String)