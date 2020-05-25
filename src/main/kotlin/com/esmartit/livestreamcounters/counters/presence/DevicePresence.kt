package com.esmartit.livestreamcounters.counters.presence

import com.esmartit.livestreamcounters.sensor.Position

data class DevicePresence(val macAddress: String, val position: Position, val time: String)

data class HourlyDevicePresenceStat(
    val time: String = "",
    val inCount: Long = 0,
    val limitCount: Long = 0,
    val outCount: Long = 0
)

data class DeviceDeltaPresence(
    val time: String,
    val increment: Position = Position.NO_POSITION,
    val decrement: Position = Position.NO_POSITION
) {
    fun isThereAChange(): Boolean {
        return increment != Position.NO_POSITION || decrement != Position.NO_POSITION
    }

    fun calculateStats(time: String, stat: HourlyDevicePresenceStat) =
        decrementDelta(time, incrementDelta(time, stat))

    private fun incrementDelta(time: String, stat: HourlyDevicePresenceStat) =
        when (increment) {
            Position.IN -> stat.copy(time = time, inCount = stat.inCount + 1)
            Position.LIMIT -> stat.copy(time = time, limitCount = stat.limitCount + 1)
            Position.OUT -> stat.copy(time = time, outCount = stat.outCount + 1)
            Position.NO_POSITION -> stat
        }

    private fun decrementDelta(time: String, stat: HourlyDevicePresenceStat) =
        when (decrement) {
            Position.IN -> stat.copy(time = time, inCount = stat.inCount - 1)
            Position.LIMIT -> stat.copy(time = time, limitCount = stat.limitCount - 1)
            Position.OUT -> stat.copy(time = time, outCount = stat.outCount - 1)
            Position.NO_POSITION -> stat
        }
}