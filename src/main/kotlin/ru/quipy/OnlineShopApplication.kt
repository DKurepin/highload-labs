package ru.quipy

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import ru.quipy.common.utils.NamedThreadFactory
import java.util.concurrent.Executors


@SpringBootApplication
class OnlineShopApplication {
    val log: Logger = LoggerFactory.getLogger(OnlineShopApplication::class.java)

    companion object {
        val appExecutor = Executors.newFixedThreadPool(64, NamedThreadFactory("main-app-executor"))
    }
}

fun main(args: Array<String>) {
    // System.setProperty("jdk.httpclient.connectionPoolSize", "10000")
   // System.setProperty("jdk.httpclient.keepalive.timeout", "60")
   // System.setProperty("jdk.httpclient.windowsize", "65536")

    runApplication<OnlineShopApplication>(*args)
}
