package com.esmartit.livestreamcounters

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class LiveStreamCountersApplication

fun main(args: Array<String>) {
    runApplication<LiveStreamCountersApplication>(*args)
}
