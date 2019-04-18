package io.axoniq.axonserver.cluster.util;

import org.junit.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class ReplicatorPeerUtilTest {

    @Test
    public void messagesDoesntFillBuffer() {
        AtomicInteger currentMessageSize = new AtomicInteger(0);
        AtomicInteger currentNumberOfMessages = new AtomicInteger(0);
        int maxMessageSize = 1000;
        int maxNumberOfMessages = 6;

        int serializedObjectSize = 100;

        IntStream.range(0,4).forEach(i -> ReplicatorPeerUtil.isOverLimit(serializedObjectSize, currentMessageSize, maxMessageSize, currentNumberOfMessages, maxNumberOfMessages));
        assertFalse(ReplicatorPeerUtil.isOverLimit(serializedObjectSize, currentMessageSize, maxMessageSize, currentNumberOfMessages, maxNumberOfMessages));
    }

    @Test
    public void lastMessageTooLarge() {
        AtomicInteger currentMessageSize = new AtomicInteger(0);
        AtomicInteger currentNumberOfMessages = new AtomicInteger(0);
        int maxMessageSize = 300;
        int maxNumberOfMessages = 6;

        int serializedObjectSize = 100;

        IntStream.range(0,4).forEach(i -> ReplicatorPeerUtil.isOverLimit(serializedObjectSize, currentMessageSize, maxMessageSize, currentNumberOfMessages, maxNumberOfMessages));
        assertTrue(ReplicatorPeerUtil.isOverLimit(serializedObjectSize, currentMessageSize, maxMessageSize, currentNumberOfMessages, maxNumberOfMessages));
    }

    @Test
    public void lastMessageOverNoOfMessageLimit() {
        AtomicInteger currentMessageSize = new AtomicInteger(0);
        AtomicInteger currentNumberOfMessages = new AtomicInteger(0);
        int maxMessageSize = 1000;
        int maxNumberOfMessages = 6;

        int serializedObjectSize = 100;

        IntStream.range(0,6).forEach(i -> ReplicatorPeerUtil.isOverLimit(serializedObjectSize, currentMessageSize, maxMessageSize, currentNumberOfMessages, maxNumberOfMessages));
        assertTrue(ReplicatorPeerUtil.isOverLimit(serializedObjectSize, currentMessageSize, maxMessageSize, currentNumberOfMessages, maxNumberOfMessages));
    }

    @Test
    public void rollsOverAfterPredicateReturnsTrue() {
        AtomicInteger currentMessageSize = new AtomicInteger(0);
        AtomicInteger currentNumberOfMessages = new AtomicInteger(0);
        int maxMessageSize = 1000;
        int maxNumberOfMessages = 6;

        int serializedObjectSize = 100;

        //fill first 6 messages reset and then add 2 more to the new buffer
        IntStream.range(0,8).forEach(i -> ReplicatorPeerUtil.isOverLimit(serializedObjectSize, currentMessageSize, maxMessageSize, currentNumberOfMessages, maxNumberOfMessages));
        assertFalse(ReplicatorPeerUtil.isOverLimit(serializedObjectSize, currentMessageSize, maxMessageSize, currentNumberOfMessages, maxNumberOfMessages));
        assertEquals(3,currentNumberOfMessages.get());
        assertEquals(300,currentMessageSize.get());
    }
}