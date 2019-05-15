package io.axoniq.axonserver.enterprise.storage.jdbc.multicontext;

import io.axoniq.axonserver.enterprise.storage.jdbc.MultiContextStrategy;
import io.axoniq.axonserver.enterprise.storage.jdbc.VendorSpecific;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Marc Gathier
 */
public class SchemaPerContextMultiContextStrategy implements MultiContextStrategy {
    private final VendorSpecific vendorSpecific;

    public SchemaPerContextMultiContextStrategy(
            VendorSpecific vendorSpecific) {
        this.vendorSpecific = vendorSpecific;
    }

    @Override
    public String getTableName(EventTypeContext eventTypeContext) {
        return schema(eventTypeContext.getContext())+ "." + table(eventTypeContext.getEventType());
    }

    @Override
    public void init( EventTypeContext eventTypeContext, Connection connection) throws SQLException {
        vendorSpecific.createSchemaIfNotExists(schema(eventTypeContext.getContext()), connection);
        vendorSpecific.createTableIfNotExists(schema(eventTypeContext.getContext()), table(eventTypeContext.getEventType()), connection);
    }

    @NotNull
    public String schema(String context) {
        return ("axon_" + context);
    }

    @NotNull
    private String table(EventType eventType) {
        return ("axon_" + eventType.name());
    }
}
