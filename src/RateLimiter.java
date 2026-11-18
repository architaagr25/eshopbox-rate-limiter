/**
 * Fixed-window rate limiter: at most {@code limit} calls are allowed per one-second window.
 *
 * One instance is meant to be shared by all threads calling the same sales channel.
 */
public class RateLimiter {

    private static final long WINDOW_NANOS = 1_000_000_000L;

    private final int limit;
    private final Clock clock;

    // Both fields are only read/written while holding the lock on "this".
    private long windowStart;
    private int count;

    public RateLimiter(int limit) {
        this(limit, System::nanoTime);
    }

    public RateLimiter(int limit, Clock clock) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be greater than 0, got " + limit);
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.limit = limit;
        this.clock = clock;
        this.windowStart = clock.nanoTime();
    }

    /**
     * Returns true if the call fits in the current second, false if it would go over the limit.
     *
     * The whole "check window -> maybe reset -> check count -> increment" sequence has to be
     * atomic, otherwise two threads can both see count == limit - 1 and both get through,
     * or one thread can reset the window while another is incrementing the old one.
     * Holding the monitor for the whole method is the simplest way to guarantee that.
     */
    public synchronized boolean allowRequest() {
        long now = clock.nanoTime();

        // nanoTime can be negative and can wrap, so always compare using the difference.
        long elapsed = now - windowStart;
        if (elapsed >= WINDOW_NANOS) {
            // Jump forward in whole seconds so windows stay aligned even after an idle gap.
            windowStart += (elapsed / WINDOW_NANOS) * WINDOW_NANOS;
            count = 0;
        }

        if (count < limit) {
            count++;
            return true;
        }
        return false;
    }

    public int getLimit() {
        return limit;
    }
}
