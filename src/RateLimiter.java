/**
 * Simple fixed-window rate limiter - allows at most `limit` calls in each 1 second window.
 * Create one per sales channel and share it across all the threads that call that channel.
 */
public class RateLimiter {

    private static final long WINDOW_NANOS = 1_000_000_000L;

    private final int limit;
    private final Clock clock;

    // only touched inside allowRequest(), which is synchronized
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
     * true  -> still under the limit for this second, go ahead
     * false -> limit already hit, caller should retry later
     *
     * Made the whole method synchronized on purpose. Checking the window, resetting it
     * and bumping the count need to happen together - if two threads both read
     * count = limit - 1 at the same time they'd both get through and we go over.
     */
    public synchronized boolean allowRequest() {
        long now = clock.nanoTime();

        // using the difference instead of comparing timestamps directly, nanoTime can overflow
        long elapsed = now - windowStart;
        if (elapsed >= WINDOW_NANOS) {
            // move ahead by full seconds, so after a long pause the windows still line up
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
