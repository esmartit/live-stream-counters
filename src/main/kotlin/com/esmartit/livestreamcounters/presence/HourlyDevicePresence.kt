package com.esmartit.livestreamcounters.presence

data class HourlyDevicePresence(val macAddress: String, val position: Position, val time: String)

enum class Position(val value: Int) {
    IN(3), LIMIT(2), OUT(1), NO_POSITION(-1)
}

data class HourlyDevicePresenceStat(
    val time: String = "",
    val inCount: Long = 0,
    val limitCount: Long = 0,
    val outCount: Long = 0
)

data class HourlyDeviceDeltaPresence(
    val time: String,
    val increment: Position = Position.NO_POSITION,
    val decrement: Position = Position.NO_POSITION
)