package com.esmartit.livestreamcounters.events

import com.esmartit.livestreamcounters.sensor.Position

data class DeviceDetectedEvent(
    val apMac: String,
    val groupName: String,
    val hotSpot: String,
    val sensorName: String,
    val spotId: String,
    val device: DeviceSeen,
    val apFloors: List<String?>,
    val countryLocation: CountryLocation? = null
)

data class DeviceSeen(
    val clientMac: String,
    val ipv4: String?,
    val ipv6: String?,
    val location: DeviceLocation,
    val manufacturer: String?,
    val os: String?,
    val rssi: Int,
    val seenEpoch: Int,
    val seenTime: String,
    val ssid: String?
)

data class DeviceLocation(
    val lat: Double?,
    val lng: Double?,
    val unc: Double?,
    val x: List<String?>,
    val y: List<String?>
)

data class CountryLocation(val countryId: String, val stateId: String, val cityId: String, val zipCode: String = "")

data class DeviceWithPresenceEvent(val deviceDetectedEvent: DeviceDetectedEvent, val position: Position) {

    fun isWithinRange(): Boolean {
        return position != Position.NO_POSITION
    }
}
