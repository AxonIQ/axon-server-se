package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventTypeContext;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Marc Gathier
 */
public class SingleSchemaMultiContextStrategy implements MultiContextStrategy {
    private final VendorSpecific vendorSpecific;

    public SingleSchemaMultiContextStrategy(VendorSpecific vendorSpecific) {
        this.vendorSpecific = vendorSpecific;
    }

    @Override
    public String getTableName(EventTypeContext eventTypeContext) {
        return eventTypeContext.getContext() + "_" + eventTypeContext.getEventType().name();
    }

    @Override
    public void init(EventTypeContext eventTypeContext, Connection connection) throws SQLException {
        vendorSpecific.createTableIfNotExists(getTableName(eventTypeContext), connection);
    }
}
