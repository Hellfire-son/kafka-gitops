// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service;

import org.apache.kafka.common.KafkaFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

/** Absolute budget for the Admin reads that form one metadata observation. */
record AdminReadDeadline(LongSupplier clock, long deadlineNanos, boolean verification) {
    static AdminReadDeadline forVerification(LongSupplier clock, long applyDeadline, long windowNanos) {
        long now = clock.getAsLong();
        // Subtract first so elapsed time is computed correctly even if nanoTime wraps.
        return new AdminReadDeadline(clock, now + Math.min(windowNanos, applyDeadline - now), true);
    }

    static AdminReadDeadline forSnapshot(int timeoutMs) {
        return new AdminReadDeadline(System::nanoTime, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs), false);
    }

    long now() {
        return clock.getAsLong();
    }

    long remainingNanos() throws InterruptedException, TimeoutException {
        if (verification && Thread.currentThread().isInterrupted()) throw new InterruptedException();
        long remaining = deadlineNanos - now();
        if (remaining <= 0) {
            if (verification) throw new Exhausted();
            throw new TimeoutException();
        }
        return remaining;
    }

    void check() throws InterruptedException, TimeoutException {
        remainingNanos();
    }

    int timeoutMs() throws InterruptedException, TimeoutException {
        long wholeMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos());
        // A verification call needs a positive integer timeout that fits in the remaining budget.
        if (verification && wholeMillis == 0) throw new Exhausted();
        return (int) Math.max(1, wholeMillis);
    }

    <T> T await(KafkaFuture<T> future) throws InterruptedException, ExecutionException, TimeoutException {
        long remaining = remainingNanos();
        T result;
        try {
            result = future.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException ex) {
            // A timed get can exhaust our budget. An exceptional Kafka reply is an ExecutionException
            // and retains its own category, including a broker or client RPC timeout.
            if (verification) check();
            throw ex;
        }
        if (verification) check();
        return result;
    }

    static final class Exhausted extends TimeoutException { }
}
