package ru.quipy.common.utils

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>(bucketSize)
    private val isDraining = AtomicBoolean(false)

    override fun tick(): Boolean {
        while (!queue.offer(1)) {
            runBlocking { waitForSlot() }
        }
        startDraining()
        return true
    }

    suspend fun waitForSlot() {
        while (queue.remainingCapacity() == 0) {
            delay(window.toMillis() / rate)
        }
    }

    private fun startDraining() {
        if (isDraining.compareAndSet(false, true)) {
            rateLimiterScope.launch {
                while (queue.isNotEmpty()) {
                    delay(window.toMillis() / rate)
                    queue.poll()
                }
                isDraining.set(false)
            }
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}
