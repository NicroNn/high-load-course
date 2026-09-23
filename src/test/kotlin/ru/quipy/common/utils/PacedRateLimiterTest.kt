package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class PacedRateLimiterTest {
    @Test
    fun `concurrent callers cannot exceed a rolling second limit`() {
        val limiter = PacedRateLimiter(5)
        val pool = Executors.newFixedThreadPool(8)
        try {
            val deadline = System.currentTimeMillis() + 10_000
            val starts = pool.invokeAll((1..12).map {
                Callable {
                    assertTrue(limiter.acquire(deadline))
                    System.nanoTime()
                }
            }).map { it.get() }.sorted()
            starts.windowed(6).forEach {
                assertTrue(it.last() - it.first() >= TimeUnit.SECONDS.toNanos(1))
            }
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `expired and too short deadlines do not consume future capacity`() {
        val limiter = PacedRateLimiter(2)
        assertFalse(limiter.acquire(System.currentTimeMillis() - 1))
        assertTrue(limiter.acquire(System.currentTimeMillis() + 100))
        assertFalse(limiter.acquire(System.currentTimeMillis() + 100))
        assertTrue(limiter.acquire(System.currentTimeMillis() + 1_000))
    }

    @Test
    fun `idle time does not accumulate a burst of permits`() {
        val limiter = PacedRateLimiter(5)
        assertTrue(limiter.acquire(System.currentTimeMillis() + 1_000))
        Thread.sleep(450)
        assertTrue(limiter.acquire(System.currentTimeMillis() + 1_000))
        assertFalse(limiter.acquire(System.currentTimeMillis() + 50))
    }

    @Test
    fun `nonpositive rates are rejected`() {
        assertThrows(IllegalArgumentException::class.java) { PacedRateLimiter(0) }
        assertThrows(IllegalArgumentException::class.java) { PacedRateLimiter(-1) }
    }
}
