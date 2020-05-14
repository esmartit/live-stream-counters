package com.esmartit.livestreamcounters.serde

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import kotlin.reflect.KClass

class JsonSerde<T : Any>(private val objectMapper: ObjectMapper, private val type: KClass<T>) : Serde<T> {

    override fun deserializer(): Deserializer<T> =
        Deserializer<T> { _, data -> objectMapper.readValue(data, type.java) }

    override fun serializer(): Serializer<T> = Serializer<T> { _, data -> objectMapper.writeValueAsBytes(data) }
}
