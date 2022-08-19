package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.file.PrimaryEventStore;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class EventWriteStorageTest {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private static TestInputStreamStorageContainer container;

    @BeforeClass
    public static void setUp() throws Exception {
        container = new TestInputStreamStorageContainer(tempFolder.getRoot(), embeddedDBProperties -> {
            embeddedDBProperties.getEvent().setSegmentsForSequenceNumberCheck(100);
            return embeddedDBProperties;
        });
        container.createDummyEvents(1000, 150, "sample");
        container.getEventWriter().clearSequenceNumberCache();
        PrimaryEventStore primaryEventStore = (PrimaryEventStore)container.getPrimary();
        while( primaryEventStore.activeSegmentCount() > 1 ) {
            Thread.sleep(10);
        }

    }

    @Test
    public void store() {
        try {
            container.createDummyEvents(1, 100, "sample");
            fail("should not be able to store new event with sequence number 0 for existing aggregate");
        } catch (MessagingPlatformException messagingPlatformException) {
            assertEquals(ErrorCode.INVALID_SEQUENCE, messagingPlatformException.getErrorCode());
        }
    }

    @AfterClass
    public static void close() {
        container.close();
    }
}