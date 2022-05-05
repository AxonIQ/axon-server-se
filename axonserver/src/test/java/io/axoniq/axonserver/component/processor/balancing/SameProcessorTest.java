package io.axoniq.axonserver.component.processor.balancing;

import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SameProcessor}
 *
 * @author Sara Pellegrini
 */
public class SameProcessorTest {

    @Test
    public void testMatch() {
        EventProcessorIdentifier id = new EventProcessorIdentifier("processorName", "context", "tokenStore");
        SameProcessor testSubject = new SameProcessor("context", id);
        ClientProcessor clientProcessor = new FakeClientProcessor("not-important",
                                                                  false, "context", EventProcessorInfo.newBuilder()
                                                                                                      .setProcessorName(
                                                                                                              "processorName")
                                                                                                      .setTokenStoreIdentifier(
                                                                                                              "tokenStore")
                                                                                                      .build());
        assertTrue(testSubject.test(clientProcessor));
    }

    @Test
    public void testNotMatch() {
        EventProcessorIdentifier id = new EventProcessorIdentifier("processorName", "context", "tokenStore");
        SameProcessor testSubject = new SameProcessor("context", id);
        ClientProcessor clientProcessor1 = new FakeClientProcessor("not-important",
                                                                   false, "context", EventProcessorInfo.newBuilder()
                                                                                                       .setProcessorName(
                                                                                                               "anotherName")
                                                                                                       .setTokenStoreIdentifier(
                                                                                                               "tokenStore")
                                                                                                       .build());
        ClientProcessor clientProcessor2 = new FakeClientProcessor("not-important",
                                                                   false, "context", EventProcessorInfo.newBuilder()
                                                                                                       .setProcessorName(
                                                                                                               "processorName")
                                                                                                       .setTokenStoreIdentifier(
                                                                                                               "anotherTokenStore")
                                                                                                       .build());
        ClientProcessor clientProcessor3 = new FakeClientProcessor("not-important",
                                                                   false, "context2", EventProcessorInfo.newBuilder()
                                                                                                        .setProcessorName(
                                                                                                                "processorName")
                                                                                                        .setTokenStoreIdentifier(
                                                                                                                "tokenStore")
                                                                                                        .build());
        assertFalse(testSubject.test(clientProcessor1));
        assertFalse(testSubject.test(clientProcessor2));
        assertFalse(testSubject.test(clientProcessor3));
    }
}