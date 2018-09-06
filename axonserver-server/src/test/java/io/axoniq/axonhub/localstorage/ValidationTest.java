package io.axoniq.axonhub.localstorage;

import io.axoniq.axonhub.localstorage.file.SegmentBasedEventStore;
import org.junit.*;
import org.junit.rules.*;

/**
 * Author: marc
 */
public class ValidationTest {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static TestStorageContainer testStorageContainer;

    @BeforeClass
    public static void init() throws Exception {
        testStorageContainer = new TestStorageContainer(tempFolder.getRoot());
        testStorageContainer.createDummyEvents(1000, 100);
    }

    @Test
    public void validate() {
        SegmentBasedEventStore primary = testStorageContainer.getPrimary();
        primary.validate(10);
    }



}
