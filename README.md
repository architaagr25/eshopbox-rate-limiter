# Eshopbox Rate Limiter

A thread-safe rate limiter in Java. It allows **at most N calls per second**, no matter how many threads call it at the same time.

```java
RateLimiter limiter = new RateLimiter(10);   // N = 10 calls per second

if (limiter.allowRequest()) {
    // call the sales-channel API
} else {
    // over the limit for this second: retry later / queue
}
```

## Project layout

```
src/
  Clock.java            time source interface (lets tests control time)
  RateLimiter.java      the rate limiter
  RateLimiterTest.java  tests, including concurrency tests
```

## How to run

Needs only a JDK (tested on Java 21, should work on Java 8+). There are no external dependencies.

```bash
javac -d out src/*.java
java -cp out RateLimiterTest
```

Expected output:

```
PASS  allows up to the limit and then rejects
PASS  resets once the second is over
PASS  idle gap longer than a second still resets cleanly
PASS  rejects invalid limit
PASS  1000 threads hitting at once -> exactly N allowed (repeated 50x)
PASS  concurrent calls while the clock moves across windows
PASS  real clock, ~3s of contention, total stays within N per window

Passed: 7, Failed: 0
```

## Approach

I used a **fixed-window counter**. Time is split into one-second windows, and the limiter keeps two fields:

- `windowStart`: when the current one-second window began
- `count`: how many calls have been allowed in this window

Each call to `allowRequest()` does this:

1. If one second or more has passed since `windowStart`, move to a new window and set `count` to 0.
2. If `count < N`, add 1 to `count` and return `true`.
3. Otherwise return `false`.

Time comes from `System.nanoTime()` rather than `System.currentTimeMillis()`. `nanoTime` only moves forward, so the limiter can't be fooled by NTP corrections or someone changing the system clock. When the window moves forward, it moves by whole seconds, so windows stay aligned even after an idle period.

## How it stays correct with many threads

The risk is a **check-then-act race**. Without protection, this can happen:

- Thread A reads `count = N-1` and decides the call is allowed.
- Before A writes `count = N`, thread B also reads `count = N-1` and also decides its call is allowed.
- Now N+1 calls have gone through.

The window reset has the same problem. Thread A sees the window has expired and sets `count = 0`. Meanwhile thread B is incrementing the old window's count. Its increment is either lost or lands in the wrong window.

To prevent this, **`allowRequest()` is `synchronized`**. The whole "read clock, maybe reset, check count, increment" sequence runs while holding the limiter's lock, so it acts as one indivisible step. Only one thread can make a decision at a time. Every thread sees an up-to-date `count`, and there's no gap between checking and incrementing. `synchronized` also guarantees that one thread's writes are visible to the next thread that takes the lock, so the fields don't need to be `volatile`.

### Why not just `AtomicInteger`?

An `AtomicInteger` makes the *increment* atomic, but the decision depends on two things together: which window we're in *and* the count in that window. `incrementAndGet()` on its own doesn't cover "reset if the window expired, then increment". Two separate atomics (one for the window, one for the count) can still race against each other.

A correct lock-free version is possible:

- Pack the window and the count into one `AtomicLong` (for example, the high bits hold the window and the low bits hold the count).
- Update it with a `compareAndSet` loop.

I didn't build it, because it's harder to read and easier to get subtly wrong. The lock is only held for a few nanoseconds of arithmetic. That's very small next to the cost of the HTTP call it protects, so contention on it shouldn't be a bottleneck here.

### How it's tested

- **Burst test:** time is frozen with a fake clock, so every call falls in the same second.
  - 1000 tasks wait on a `CountDownLatch` and are released together to get as much contention as possible.
  - Exactly N must succeed, no more and no less.
  - The test repeats 50 times to catch rare interleavings.
- **Window-boundary test:** worker threads hammer the limiter while another thread pushes the fake clock forward one second at a time. This exercises the reset path under contention. Each success is put in its window's bucket, and no bucket may go over N.
- **Real-clock test:** 32 threads spin for about 3 seconds against the real clock. The total number of allowed calls must not exceed N × (number of windows touched).
- **Single-threaded tests:** these check the basic behaviour: the limit, the reset at exactly one second, a long idle gap, and invalid input.

## Things I was unsure about / trade-offs

1. **Bursts at window boundaries.** A fixed window guarantees at most N calls per *calendar* second. It does **not** guarantee N calls in *any* 1-second span: N calls at 0.99s and N more at 1.01s means 2N calls within about 20ms. I read the requirement as "N per second", which this meets. If a channel counts over a rolling window instead, I'd switch to one of these:
   - a **sliding log**: keep the timestamps of the last N allowed calls, and allow a call only if the oldest one is more than 1s old;
   - a **token bucket** (smooth refill).

   Both keep the same synchronized structure, only the internal state changes.
2. **Clock drift with the channel.** Our idea of "this second" and the channel's idea won't line up exactly. To be safe in production I'd probably configure the limit a bit below the channel's published value.
3. **One process only.** This limiter is in memory, so it only protects calls from a single JVM. If inventory sync runs on several instances, the limit has to live in a shared store, for example a Redis `INCR` with a 1-second `EXPIRE` on a key like `ratelimit:{channel}:{epochSecond}`, or a Lua script to make the check and increment atomic.
4. **One limiter per channel.** Each sales channel has its own N, so in practice there would be one instance per channel, for example `ConcurrentHashMap<String, RateLimiter>` with `computeIfAbsent`.
5. **Reject vs. wait.** `allowRequest()` never blocks. It returns `false` right away and the caller decides whether to retry, back off, or queue. A blocking `acquire()` could be added on top if callers would rather wait.
6. **Fairness.** `synchronized` isn't fair, so under heavy contention some threads may be rejected more often than others. The total is still correct. If fairness mattered, a `ReentrantLock(true)` would fix it, at the cost of some throughput.
