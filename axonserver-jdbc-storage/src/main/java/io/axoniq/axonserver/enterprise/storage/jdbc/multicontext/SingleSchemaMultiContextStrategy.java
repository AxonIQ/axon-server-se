package io.axoniq.axonserver.enterprise.storage.jdbc.multicontext;

import io.axoniq.axonserver.enterprise.storage.jdbc.MultiContextStrategy;
import io.axoniq.axonserver.enterprise.storage.jdbc.VendorSpecific;
import io.axoniq.axonserver.localstorage.EventTypeContext;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * This strategy uses a single database schema for all contexts for storing events. Implementation depends on capabilities
 * of the RDBMS type.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class SingleSchemaMultiContextStrategy implements MultiContextStrategy {
    private final VendorSpecific vendorSpecific;

    public SingleSchemaMultiContextStrategy(VendorSpecific vendorSpecific) {
        this.vendorSpecific = vendorSpecific;
    }

    @Override
    public String getTableName(EventTypeContext eventTypeContext) {
        return vendorSpecific.fullyQualified(tableName(eventTypeContext));
    }
    private String tableName(EventTypeContext eventTypeContext) {
        return eventTypeContext.getContext() + "_" + eventTypeContext.getEventType().name();
    }

    @Override
    public void init(EventTypeContext eventTypeContext, Connection connection) throws SQLException {
        vendorSpecific.createTableIfNotExists(tableName(eventTypeContext), connection);
    }
}
