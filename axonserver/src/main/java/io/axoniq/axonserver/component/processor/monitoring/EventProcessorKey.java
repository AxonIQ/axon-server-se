package io.axoniq.axonserver.component.processor.monitoring;

import io.micrometer.core.instrument.Tags;

/**
 * Represents a unique event processor, based on the context, component and name.
 *
 * @author Mitchell Herrijgers
 */
public class EventProcessorKey {
    private final String context;
    private final String component;
    private final String name;

    public EventProcessorKey(String context, String component, String name) {
        this.context = context;
        this.component = component;
        this.name = name;
    }

    public Tags asMetricTags() {
        return Tags
                .of("context", context)
                .and("component", component)
                .and("name", name);
    }

    public String getContext() {
        return context;
    }

    public String getComponent() {
        return component;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final EventProcessorKey that = (EventProcessorKey) o;

        if (!context.equals(that.context)) return false;
        if (!component.equals(that.component)) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = context.hashCode();
        result = 31 * result + component.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
