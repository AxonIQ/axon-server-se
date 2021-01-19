package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link SequenceValidationStreamObserver}.
 *
 * @author Milan Savic
 */
public class SequenceValidationStreamObserverTest {

    private SequenceValidationStreamObserver testSubject;
    private StreamObserver<SerializedEvent> delegateMock;

    @Before
    public void setup() {
        delegateMock = mock(StreamObserver.class);
        testSubject = new SequenceValidationStreamObserver(delegateMock);
    }

    @Test
    public void testValidSequence() {
        SerializedEvent event1 = serializedEvent(0);
        SerializedEvent event2 = serializedEvent(1);
        SerializedEvent event3 = serializedEvent(2);
        SerializedEvent event4 = serializedEvent(3);
        SerializedEvent event5 = serializedEvent(4);
        testSubject.onNext(event1);
        testSubject.onNext(event2);
        testSubject.onNext(event3);
        testSubject.onNext(event4);
        testSubject.onNext(event5);
        testSubject.onCompleted();
        verify(delegateMock).onNext(event1);
        verify(delegateMock).onNext(event2);
        verify(delegateMock).onNext(event3);
        verify(delegateMock).onNext(event4);
        verify(delegateMock).onNext(event5);
        verify(delegateMock).onCompleted();
    }

    @Test
    public void testValidSequenceErrorsOut() {
        SerializedEvent event1 = serializedEvent(0);
        SerializedEvent event2 = serializedEvent(1);
        SerializedEvent event3 = serializedEvent(2);
        SerializedEvent event4 = serializedEvent(3);
        SerializedEvent event5 = serializedEvent(4);
        testSubject.onNext(event1);
        testSubject.onNext(event2);
        testSubject.onNext(event3);
        testSubject.onNext(event4);
        testSubject.onNext(event5);
        RuntimeException throwable = new RuntimeException();
        testSubject.onError(throwable);
        verify(delegateMock).onNext(event1);
        verify(delegateMock).onNext(event2);
        verify(delegateMock).onNext(event3);
        verify(delegateMock).onNext(event4);
        verify(delegateMock).onNext(event5);
        verify(delegateMock).onError(throwable);
    }

    @Test
    public void testInvalidSequence() {
        SerializedEvent event1 = serializedEvent(0);
        SerializedEvent event2 = serializedEvent(1);
        SerializedEvent event4 = serializedEvent(3);
        testSubject.onNext(event1);
        testSubject.onNext(event2);
        testSubject.onNext(event4);
        verify(delegateMock).onNext(event1);
        verify(delegateMock).onNext(event2);
        verify(delegateMock).onError(any(RuntimeException.class));
    }

    @Test
    public void testRepeatedSequence() {
        SerializedEvent event1 = serializedEvent(0);
        SerializedEvent event2 = serializedEvent(1);
        testSubject.onNext(event1);
        testSubject.onNext(event2);
        testSubject.onNext(event2);
        verify(delegateMock).onNext(event1);
        verify(delegateMock).onNext(event2);
        verify(delegateMock).onError(any(RuntimeException.class));
    }

    @NotNull
    private SerializedEvent serializedEvent(long aggregateSequenceNumber) {
        return new SerializedEvent(Event.newBuilder()
                                        .setAggregateSequenceNumber(aggregateSequenceNumber)
                                        .build());
    }
}