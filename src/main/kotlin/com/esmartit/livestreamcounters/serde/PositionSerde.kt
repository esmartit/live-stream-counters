package com.esmartit.livestreamcounters.serde

import com.esmartit.livestreamcounters.sensor.Position
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class PositionSerde :
    Serde<Position> {
    override fun deserializer(): Deserializer<Position> {
        return Deserializer<Position> { _, data -> Position.valueOf(String(data)) }
    }

    override fun serializer(): Serializer<Position> {
        return Serializer<Position> { _, data -> data.name.toByteArray() }
    }
}