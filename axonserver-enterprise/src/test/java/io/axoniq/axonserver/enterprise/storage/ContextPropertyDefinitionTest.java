package io.axoniq.axonserver.enterprise.storage;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ContextPropertyDefinitionTest {

    @Test
    public void apply() {
        StorageProperties storageProperties = new StorageProperties(new SystemInfoProvider() {
        });
        storageProperties = ContextPropertyDefinition.EVENT_SEGMENT_SIZE.apply(storageProperties, "12345");
        assertEquals(12345L, storageProperties.getSegmentSize());
    }

    @Test
    public void validate() {
        ContextPropertyDefinition.EVENT_SEGMENT_SIZE.validate("12345");
    }

    @Test
    public void validateInvalidLong() {
        try {
            ContextPropertyDefinition.EVENT_SEGMENT_SIZE.validate("big");
            fail("Validation should fail");
        } catch (IllegalArgumentException ex) {
            // Expect number format exception
        }
    }

    @Test
    public void validateInvalidInt() {
        try {
            ContextPropertyDefinition.EVENT_MAX_BLOOM_FILTERS_IN_MEMORY.validate("many");
            fail("Validation should fail");
        } catch (IllegalArgumentException ex) {
            // Expect number format exception
        }
    }

    @Test
    public void validateInvalidDuration() {
        try {
            ContextPropertyDefinition.EVENT_INDEX_FORMAT.validate("INVALID_OPTION");
            fail("Validation should fail");
        } catch (IllegalArgumentException ex) {
            // Expect illegal argument exception
        }
    }

    @Test
    public void findByKey() {
        assertNotNull(ContextPropertyDefinition.findByKey("event.storage"));
    }

    @Test
    public void findByKeNotFound() {
        assertNull(ContextPropertyDefinition.findByKey("event.storage.location"));
    }
}