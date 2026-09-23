package ru.quipy.common.utils

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/** Spaces requests evenly, including after idle periods; unused capacity is not accumulated. */
class PacedRateLimiter(ratePerSecond: Int) {
    init {
        require(ratePerSecond > 0) { "ratePerSecond must be positive" }
    }

    // Leave 10% interval headroom for network/GC jitter at the provider's window boundary.
    private val intervalNanos = (1_100_000_000L + ratePerSecond - 1) / ratePerSecond
    private val lock = ReentrantLock(true)
    private var nextStartNanos = System.nanoTime()

    /** latestStartMillis is an absolute Unix timestamp, not a request duration. */
    fun acquire(latestStartMillis: Long): Boolean {
        val budgetMillis = latestStartMillis - System.currentTimeMillis()
        if (budgetMillis <= 0 || !lock.tryLock(budgetMillis, TimeUnit.MILLISECONDS)) return false
        try {
            while (true) {
                val remainingMillis = latestStartMillis - System.currentTimeMillis()
                if (remainingMillis <= 0) return false
                val waitNanos = nextStartNanos - System.nanoTime()
                if (waitNanos <= 0) {
                    nextStartNanos = System.nanoTime() + intervalNanos
                    return true
                }
                if (waitNanos >= TimeUnit.MILLISECONDS.toNanos(remainingMillis)) return false
                TimeUnit.NANOSECONDS.sleep(waitNanos)
            }
        } finally {
            lock.unlock()
        }
    }
}
