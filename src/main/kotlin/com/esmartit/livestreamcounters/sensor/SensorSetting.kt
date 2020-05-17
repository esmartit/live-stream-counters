package com.esmartit.livestreamcounters.sensor

data class SensorSetting(
    val id: String,
    val spot: String = "default",
    val sensorId: String = "default",
    val location: String = "default",
    val inEdge: Int = -35,
    val limitEdge: Int = -45,
    val outEdge: Int = -300
) {
    fun presence(power: Int) = when {
        power >= inEdge -> Position.IN
        power >= limitEdge -> Position.LIMIT
        power >= outEdge -> Position.OUT
        else -> Position.NO_POSITION
    }
}