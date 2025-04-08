package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.pow

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val maxRetries = 10

    private val timeout = Duration.ofSeconds(2)

    val connectionPool = ConnectionPool(maxIdleConnections = 1000, keepAliveDuration = 5, TimeUnit.MINUTES)
    val dispatcher = Dispatcher().apply {
        maxRequests = 10000
        maxRequestsPerHost = 1000
    }

    private val client = OkHttpClient.Builder()
        .callTimeout(timeout)
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .connectionPool(connectionPool)
        .dispatcher(dispatcher)
        .build()

    private val rateLimiter = FixedWindowRateLimiter(
        rate = rateLimitPerSec,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )

    private val parallelRequestsSemaphore = Semaphore(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        GlobalScope.launch(Dispatchers.IO) {
            performPaymentInternal(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private suspend fun performPaymentInternal(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submitting payment request for payment $paymentId, txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        parallelRequestsSemaphore.acquire()
        try {
            if (!rateLimiter.tick()) {
                rateLimiter.tickBlocking()
            }

            var retryCount = 0
            var isSuccessful = false
            while (retryCount < maxRetries && !isSuccessful) {
                if (now() >= deadline) {
                    logger.error("[$accountName] Deadline exceeded for payment $paymentId, txId: $transactionId")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                    }
                    return
                }

                try {
                    val response = client.newCall(request).execute()
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    isSuccessful = body.result // если успешный ответ, выходим из цикла
                } catch (e: Exception) {
                    if (retryCount >= maxRetries - 1) {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId after $retryCount retries.", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Max retries reached.")
                        }
                        break
                    }

                    val delayTime = 100L * (2.0.pow(retryCount.toDouble())).toLong()
                    logger.warn("[$accountName] Retrying payment for txId: $transactionId, payment: $paymentId, attempt: ${retryCount + 1}")
                    delay(delayTime)
                    retryCount++
                }
            }
        } finally {
            parallelRequestsSemaphore.release()
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
