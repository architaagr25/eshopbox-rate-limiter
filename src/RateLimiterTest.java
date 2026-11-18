import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Plain-Java tests (no JUnit) so the project runs with just a JDK.
 *
 *   javac -d out src/*.java
 *   java -cp out RateLimiterTest
 */
public class RateLimiterTest {

    private static final long ONE_SECOND = 1_000_000_000L;

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws Exception {
        run("allows up to the limit and then rejects", RateLimiterTest::allowsUpToLimitThenRejects);
        run("resets once the second is over", RateLimiterTest::resetsAfterWindow);
        run("idle gap longer than a second still resets cleanly", RateLimiterTest::handlesLongIdleGap);
        run("rejects invalid limit", RateLimiterTest::rejectsInvalidLimit);
        run("1000 threads hitting at once -> exactly N allowed (repeated 50x)",
                RateLimiterTest::concurrentCallsNeverExceedLimit);
        run("concurrent calls while the clock moves across windows",
                RateLimiterTest::concurrentCallsAcrossWindowBoundaries);
        run("real clock, ~3s of contention, total stays within N per window",
                RateLimiterTest::realClockStress);

        System.out.println();
        System.out.println("Passed: " + passed + ", Failed: " + failed);
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------------
    // Single-threaded behaviour
    // ---------------------------------------------------------------------

    static void allowsUpToLimitThenRejects() {
        FakeClock clock = new FakeClock();
        RateLimiter limiter = new RateLimiter(5, clock);

        for (int i = 0; i < 5; i++) {
            check(limiter.allowRequest(), "call " + (i + 1) + " should be allowed");
        }
        check(!limiter.allowRequest(), "6th call should be rejected");
        check(!limiter.allowRequest(), "7th call should be rejected");
    }

    static void resetsAfterWindow() {
        FakeClock clock = new FakeClock();
        RateLimiter limiter = new RateLimiter(3, clock);

        for (int i = 0; i < 3; i++) limiter.allowRequest();
        check(!limiter.allowRequest(), "should be blocked once limit is used up");

        clock.advance(ONE_SECOND - 1);
        check(!limiter.allowRequest(), "still the same second, should be blocked");

        clock.advance(1);
        check(limiter.allowRequest(), "new second, should be allowed again");
    }

    static void handlesLongIdleGap() {
        FakeClock clock = new FakeClock();
        RateLimiter limiter = new RateLimiter(2, clock);

        limiter.allowRequest();
        limiter.allowRequest();

        // jump 10.5 seconds ahead; we should land in a fresh window aligned to whole seconds
        clock.advance(10 * ONE_SECOND + ONE_SECOND / 2);
        check(limiter.allowRequest(), "first call after idle gap");
        check(limiter.allowRequest(), "second call after idle gap");
        check(!limiter.allowRequest(), "third call after idle gap should be blocked");

        // the window started at t=10s, so it should end at t=11s, not t=11.5s
        clock.advance(ONE_SECOND / 2);
        check(limiter.allowRequest(), "window should roll over at the whole-second boundary");
    }

    static void rejectsInvalidLimit() {
        try {
            new RateLimiter(0);
            check(false, "limit 0 should throw");
        } catch (IllegalArgumentException expected) {
            // ok
        }
        try {
            new RateLimiter(-3);
            check(false, "negative limit should throw");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    // ---------------------------------------------------------------------
    // Concurrency
    // ---------------------------------------------------------------------

    /**
     * Time is frozen, so every call falls in the same second. All threads wait on a latch
     * and are released together to maximise contention. Exactly N must succeed.
     */
    static void concurrentCallsNeverExceedLimit() throws Exception {
        int limit = 10;
        int threads = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(64);
        try {
            for (int round = 0; round < 50; round++) {
                RateLimiter limiter = new RateLimiter(limit, new FakeClock());
                AtomicInteger allowed = new AtomicInteger();
                CountDownLatch ready = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();

                for (int i = 0; i < threads; i++) {
                    futures.add(pool.submit(() -> {
                        ready.await();
                        if (limiter.allowRequest()) {
                            allowed.incrementAndGet();
                        }
                        return null;
                    }));
                }
                ready.countDown();
                for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);

                check(allowed.get() == limit,
                        "round " + round + ": expected " + limit + " allowed, got " + allowed.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Worker threads hammer the limiter while another thread pushes the fake clock forward
     * one second at a time. This exercises the reset path under contention. Each successful
     * call records which window it landed in (read under the same lock, so it's exact),
     * and no window may have more than N.
     */
    static void concurrentCallsAcrossWindowBoundaries() throws Exception {
        int limit = 20;
        int windows = 20;
        int workers = 16;
        FakeClock clock = new FakeClock();
        RateLimiter limiter = new RateLimiter(limit, clock);
        AtomicInteger[] perWindow = new AtomicInteger[windows + 1];
        for (int i = 0; i < perWindow.length; i++) perWindow[i] = new AtomicInteger();

        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger windowsDone = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        List<Future<?>> futures = new ArrayList<>();

        for (int w = 0; w < workers; w++) {
            futures.add(pool.submit(() -> {
                start.await();
                while (windowsDone.get() <= windows) {
                    // Hold the limiter's lock while reading the clock so the window index we
                    // record is the same one the limiter used for this decision.
                    synchronized (limiter) {
                        if (limiter.allowRequest()) {
                            int idx = (int) (clock.nanoTime() / ONE_SECOND);
                            perWindow[Math.min(idx, windows)].incrementAndGet();
                        }
                    }
                }
                return null;
            }));
        }

        start.countDown();
        for (int i = 0; i < windows; i++) {
            Thread.sleep(5);
            clock.advance(ONE_SECOND);
            windowsDone.incrementAndGet();
        }
        Thread.sleep(5);
        windowsDone.incrementAndGet();
        for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);
        pool.shutdownNow();

        for (int i = 0; i <= windows; i++) {
            int got = perWindow[i].get();
            check(got <= limit, "window " + i + " allowed " + got + " calls, limit is " + limit);
        }
    }

    /**
     * Uses the real clock. Threads spin for ~3 seconds; we can't know the exact window of
     * each call from outside, so we check the total: over k windows, at most k * N calls
     * may succeed.
     */
    static void realClockStress() throws Exception {
        int limit = 50;
        int workers = 32;
        long runNanos = 3 * ONE_SECOND;
        RateLimiter limiter = new RateLimiter(limit);
        AtomicInteger allowed = new AtomicInteger();
        AtomicLong endAt = new AtomicLong();
        CountDownLatch start = new CountDownLatch(1);

        long begin = System.nanoTime();
        endAt.set(begin + runNanos);

        ExecutorService pool = Executors.newFixedThreadPool(workers);
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            futures.add(pool.submit(() -> {
                start.await();
                while (System.nanoTime() < endAt.get()) {
                    if (limiter.allowRequest()) allowed.incrementAndGet();
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        pool.shutdownNow();

        long elapsed = System.nanoTime() - begin;
        long windowsTouched = elapsed / ONE_SECOND + 1;
        long maxAllowed = windowsTouched * limit;
        check(allowed.get() <= maxAllowed,
                "allowed " + allowed.get() + " calls over " + windowsTouched + " windows, max is " + maxAllowed);
        check(allowed.get() >= 3L * limit,
                "expected at least " + (3 * limit) + " allowed over 3s, got " + allowed.get());
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    /** Manually controlled clock. Starts at 0. */
    static class FakeClock implements Clock {
        private final AtomicLong now = new AtomicLong();

        @Override
        public long nanoTime() {
            return now.get();
        }

        void advance(long nanos) {
            now.addAndGet(nanos);
        }
    }

    interface TestBody {
        void run() throws Exception;
    }

    private static void run(String name, TestBody body) {
        try {
            body.run();
            passed++;
            System.out.println("PASS  " + name);
        } catch (Throwable t) {
            failed++;
            System.out.println("FAIL  " + name + " -> " + t.getMessage());
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
