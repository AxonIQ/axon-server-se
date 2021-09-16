package io.axoniq.axonserver.component.processor.balancing;

import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link SameProcessor}
 *
 * @author Sara Pellegrini
 */
public class SameProcessorTest {

    @Test
    public void testMatch() {
        EventProcessorIdentifier id = new EventProcessorIdentifier("processorName", "tokenStore");
        SameProcessor testSubject = new SameProcessor(id);
        ClientProcessor clientProcessor = new FakeClientProcessor("not-important",
                                                                  false, EventProcessorInfo.newBuilder()
                                                                                           .setProcessorName(
                                                                                                   "processorName")
                                                                                           .setTokenStoreIdentifier(
                                                                                                   "tokenStore")
                                                                                           .build());
        assertTrue(testSubject.test(clientProcessor));
    }

    @Test
    public void testNotMatch() {
        EventProcessorIdentifier id = new EventProcessorIdentifier("processorName", "tokenStore");
        SameProcessor testSubject = new SameProcessor(id);
        ClientProcessor clientProcessor1 = new FakeClientProcessor("not-important",
                                                                   false, EventProcessorInfo.newBuilder()
                                                                                            .setProcessorName(
                                                                                                    "anotherName")
                                                                                            .setTokenStoreIdentifier(
                                                                                                    "tokenStore")
                                                                                            .build());
        ClientProcessor clientProcessor2 = new FakeClientProcessor("not-important",
                                                                   false, EventProcessorInfo.newBuilder()
                                                                                            .setProcessorName(
                                                                                                    "processorName")
                                                                                            .setTokenStoreIdentifier(
                                                                                                    "anotherTokenStore")
                                                                                            .build());

        assertFalse(testSubject.test(clientProcessor1));
        assertFalse(testSubject.test(clientProcessor2));
    }
}