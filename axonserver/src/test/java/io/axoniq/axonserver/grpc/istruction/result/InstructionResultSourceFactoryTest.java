package io.axoniq.axonserver.grpc.istruction.result;

import io.axoniq.axonserver.grpc.InstructionResult;
import org.junit.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link InstructionResultSourceFactory}
 *
 * @author Sara Pellegrini
 */
public class InstructionResultSourceFactoryTest {

    private List<Consumer<InstructionResult>> instructionResultListeners = new ArrayList<>();

    private InstructionResultSourceFactory testSubject = new InstructionResultSourceFactory(instructionResultListeners::add,
                                                                                            10);

    @Test
    public void testOnSuccess() {
        AtomicBoolean successReceived = new AtomicBoolean(false);
        AtomicBoolean failureReceived = new AtomicBoolean(false);
        InstructionResultSource instructionResultSource = testSubject.onInstructionResultFor("MyInstructionId");
        instructionResultSource.subscribe(() -> successReceived.set(true), error -> failureReceived.set(true));
        notifyInstructionResultReceived(InstructionResult.newBuilder()
                                                         .setInstructionId("AnotherId")
                                                         .build());

        assertFalse(successReceived.get());
        assertFalse(failureReceived.get());

        notifyInstructionResultReceived(InstructionResult.newBuilder()
                                                         .setInstructionId("MyInstructionId")
                                                         .setSuccess(true)
                                                         .build());

        assertTrue(successReceived.get());
        assertFalse(failureReceived.get());
    }

    @Test
    public void testOnFailure() {
        AtomicBoolean successReceived = new AtomicBoolean(false);
        AtomicBoolean failureReceived = new AtomicBoolean(false);
        AtomicBoolean timeoutReceived = new AtomicBoolean(false);
        InstructionResultSource instructionResultSource = testSubject.onInstructionResultFor("MyInstructionId");
        instructionResultSource.subscribe(() -> successReceived.set(true),
                                          error -> failureReceived.set(true),
                                          timout -> timeoutReceived.set(true));
        notifyInstructionResultReceived(InstructionResult.newBuilder()
                                                         .setInstructionId("AnotherId")
                                                         .build());

        assertFalse(successReceived.get());
        assertFalse(failureReceived.get());
        assertFalse(timeoutReceived.get());

        notifyInstructionResultReceived(InstructionResult.newBuilder()
                                                         .setInstructionId("MyInstructionId")
                                                         .setSuccess(false)
                                                         .build());

        assertFalse(successReceived.get());
        assertTrue(failureReceived.get());
        assertFalse(timeoutReceived.get());
    }

    @Test
    public void testOnTimeout() throws InterruptedException {
        AtomicBoolean successReceived = new AtomicBoolean(false);
        AtomicBoolean failureReceived = new AtomicBoolean(false);
        AtomicBoolean timeoutReceived = new AtomicBoolean(false);
        InstructionResultSource instructionResultSource = testSubject.onInstructionResultFor("MyInstructionId");
        instructionResultSource.subscribe(() -> successReceived.set(true),
                                          error -> failureReceived.set(true),
                                          timout -> timeoutReceived.set(true),
                                          Duration.ofSeconds(0));
        Thread.sleep(50);
        assertFalse(successReceived.get());
        assertFalse(failureReceived.get());
        assertWithin(100, TimeUnit.MILLISECONDS, () -> assertTrue(timeoutReceived.get()));

        notifyInstructionResultReceived(InstructionResult.newBuilder()
                                                         .setInstructionId("MyInstructionId")
                                                         .setSuccess(true)
                                                         .build());

        assertFalse(successReceived.get());
        assertFalse(failureReceived.get());
    }

    private void notifyInstructionResultReceived(InstructionResult instructionResult) {
        instructionResultListeners.forEach(listener -> listener.accept(instructionResult));
    }
}