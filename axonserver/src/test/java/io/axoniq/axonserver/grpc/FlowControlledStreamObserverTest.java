package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.util.CountingStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class FlowControlledStreamObserverTest {
    private FlowControlledStreamObserver<String> testSubject;
    private AtomicReference<Throwable> errorReference = new AtomicReference<>();
    private CountingStreamObserver<String> delegate = new CountingStreamObserver<>();

    @Before
    public void setUp() throws Exception {
        testSubject = new FlowControlledStreamObserver<>(delegate, t -> errorReference.set(t));
        testSubject.addPermits(10);
    }

    @Test
    public void testNoMorePermits() {
        IntStream.range(0, 20).forEach(i -> testSubject.onNext("Sample"));
        assertEquals(10, delegate.count);
        assertEquals(IllegalStateException.class, errorReference.get().getClass());
        testSubject.addPermits(10);
        IntStream.range(0, 20).forEach(i -> testSubject.onNext("Sample"));
        assertEquals(20, delegate.count);
    }
}