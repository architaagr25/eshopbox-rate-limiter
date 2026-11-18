/**
 * Small time source abstraction so tests can control time instead of sleeping.
 * Production code uses System::nanoTime.
 */
@FunctionalInterface
public interface Clock {
    long nanoTime();
}
