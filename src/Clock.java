/**
 * Wrapped the time source in an interface so the tests can use a fake clock
 * instead of actually sleeping. In normal use it's just System::nanoTime.
 */
@FunctionalInterface
public interface Clock {
    long nanoTime();
}
