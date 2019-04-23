package io.axoniq.axonserver.cluster.util;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.junit.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class MaxMessageSizePredicateTest {

    @Test
    public void messagesDoesntFillBuffer() {
        MaxMessageSizePredicate predicate = new MaxMessageSizePredicate(1000,6);

        Integer serializedObjectSize = 100;

        IntStream.range(0,4).forEach(i -> predicate.test(serializedObjectSize));
        assertFalse(predicate.test(serializedObjectSize));
    }

    @Test
    public void lastMessageTooLarge() {
        MaxMessageSizePredicate predicate = new MaxMessageSizePredicate(300,6);

        Integer serializedObjectSize = 100;

        IntStream.range(0,4).forEach(i -> predicate.test(serializedObjectSize));
        assertTrue(predicate.test(serializedObjectSize));
    }

    @Test
    public void lastMessageOverNoOfMessageLimit() {
        MaxMessageSizePredicate predicate = new MaxMessageSizePredicate(1000,6);

        Integer serializedObjectSize = 100;

        IntStream.range(0,6).forEach(i -> predicate.test(serializedObjectSize));
        assertTrue(predicate.test(serializedObjectSize));
    }

    @Test
    public void rollsOverAfterPredicateReturnsTrue() {
        MaxMessageSizePredicate predicate = new MaxMessageSizePredicate(1000,6);

        Integer serializedObjectSize = 100;

        //fill first 6 messages reset and then add 2 more to the new buffer
        IntStream.range(0,8).forEach(i -> predicate.test(serializedObjectSize));
        assertFalse(predicate.test(serializedObjectSize));
    }
}