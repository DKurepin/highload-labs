package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.async.HttpAsyncClients
import kotlinx.coroutines.*
import liquibase.pro.packaged.ex
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.ProxySelector
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.pow
import kotlin.toString

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
    private val maxRetries = 2
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)


    private val timeout = Duration.ofSeconds(2)


    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        //.executor(Executors.newFixedThreadPool(parallelRequests))
        .priority(1)
        .build()


    private val rateLimiter = FixedWindowRateLimiter(
        rate = rateLimitPerSec,
        window = 1500,
        timeUnit = TimeUnit.SECONDS
    )

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(1500, Duration.ofSeconds(1))


    private val parallelRequestsSemaphore = Semaphore(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request: HttpRequest = HttpRequest.newBuilder()
            .uri(URI("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        if (!parallelRequestsSemaphore.tryAcquire()) {
            logger.error("[$accountName] [ERROR] Too many concurrent requests.")
            return
        }
        try {
            if (now() + properties.averageProcessingTime.toMillis() >= deadline) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return
            }
            while (!slidingWindowRateLimiter.tick()) {
                logger.error("[$accountName] [ERROR] Sliding window rate limit.")
                return
            }
            while (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
                logger.error("[$accountName] [ERROR] Ongoing window limit.")
                return
            }

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAcceptAsync{ response ->
                    try {
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }

                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Error processing payment response for paymentId: $paymentId, txId: $transactionId", e)
                    }
                }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error(
                        "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                        e
                    )
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            parallelRequestsSemaphore.release()
            ongoingWindow.releaseWindow()
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
