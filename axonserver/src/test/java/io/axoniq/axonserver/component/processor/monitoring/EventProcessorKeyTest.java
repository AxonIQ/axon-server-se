package io.axoniq.axonserver.component.processor.monitoring;

import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EventProcessorKeyTest {
    @Test
    void hashCodeAndEqualsWorkCorrectly() {
        final EventProcessorKey baseKey = new EventProcessorKey("context", "component", "name");
        final EventProcessorKey sameKey = new EventProcessorKey("context", "component", "name");
        final EventProcessorKey otherKey = new EventProcessorKey("context", "component2", "otherName");

        assertEquals(baseKey, sameKey);
        assertNotEquals(baseKey, otherKey);

        assertEquals(baseKey.hashCode(), sameKey.hashCode());
        assertNotEquals(baseKey.hashCode(), otherKey.hashCode());
    }

    @Test
    void createsCorrectTags() {
        final EventProcessorKey key = new EventProcessorKey("default", "my-microservice", "processor-name");
        final Tags tags = key.asMetricTags();

        assertTrue(tags.stream().anyMatch(tag -> tag.getKey().equals("context") && tag.getValue().equals("default")));
        assertTrue(tags.stream().anyMatch(tag -> tag.getKey().equals("component") && tag.getValue().equals("my-microservice")));
        assertTrue(tags.stream().anyMatch(tag -> tag.getKey().equals("name") && tag.getValue().equals("processor-name")));
    }
}
