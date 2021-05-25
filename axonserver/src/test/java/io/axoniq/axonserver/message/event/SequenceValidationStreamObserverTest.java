package io.axoniq.axonserver.message.event;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.grpc.stub.CallStreamObserver;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link SequenceValidationStreamObserver}.
 *
 * @author Milan Savic
 */
public class SequenceValidationStreamObserverTest {

    private SequenceValidationStreamObserver testSubject;
    private CallStreamObserver<SerializedEvent> delegateMock;
    private String context = "myContext";

    @Before
    public void setup() {
        delegateMock = mock(CallStreamObserver.class);
        testSubject = new SequenceValidationStreamObserver(delegateMock, SequenceValidationStrategy.FAIL, context);
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
    public void testInvalidSequenceLogOnly() {
        testSubject = new SequenceValidationStreamObserver(delegateMock, SequenceValidationStrategy.LOG, context);
        SerializedEvent event1 = serializedEvent(0);
        SerializedEvent event2 = serializedEvent(1);
        SerializedEvent event4 = serializedEvent(3);
        testSubject.onNext(event1);
        testSubject.onNext(event2);
        testSubject.onNext(event4);
        verify(delegateMock).onNext(event1);
        verify(delegateMock).onNext(event2);
        verify(delegateMock).onNext(event4);
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

    @Test
    public void testLogging() {
        Logger logger = (Logger) LoggerFactory.getLogger(SequenceValidationStreamObserver.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
        testInvalidSequence();
        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.get(0).getMessage().contains(context));
        assertEquals(Level.INFO, logsList.get(0).getLevel());
    }
}