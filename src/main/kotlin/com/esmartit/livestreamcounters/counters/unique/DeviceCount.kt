package com.esmartit.livestreamcounters.counters.unique

data class DeviceCount(val count: Long, val time: Long)
data class ShouldIncreaseCount(val value: Boolean)