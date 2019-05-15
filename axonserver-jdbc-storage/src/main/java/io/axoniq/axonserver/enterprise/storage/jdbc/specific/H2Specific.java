package io.axoniq.axonserver.enterprise.storage.jdbc.specific;

import io.axoniq.axonserver.enterprise.storage.jdbc.JdbcUtils;
import io.axoniq.axonserver.enterprise.storage.jdbc.VendorSpecific;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Creation and deletion of database objects specific for a H2 database.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class H2Specific implements VendorSpecific {

    @Override
    public void createTableIfNotExists(String tableName, Connection connection) throws SQLException {
        createTableIfNotExists(null, tableName, connection);
    }

    @Override
    public void createTableIfNotExists(String schema, String table, Connection connection) throws SQLException {
        try (ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), schema, table, null)) {
            if( resultSet.next()) {
                return;
            }
        }

        String createTable = String.format(
                "create table %s ("
                        + "global_index bigint not null, "
                        + "aggregate_identifier varchar(255) not null, "
                        + "event_identifier varchar(255) not null, "
                        + "meta_data blob, payload blob not null, "
                        + "payload_revision varchar(255), "
                        + "payload_type varchar(255) not null, "
                        + "sequence_number bigint not null, "
                        + "time_stamp bigint not null, "
                        + "type varchar(255), "
                        + "primary key (global_index))",
                fullyQualifiedName(schema, table));
        String createIndexAggidSeqnr = String.format(
                "alter table %s add constraint %s_uk1 unique (aggregate_identifier, sequence_number)",
                fullyQualifiedName(schema, table),
                table);
        String createIndexEventId = String.format(
                "alter table %s add constraint %s_uk2 unique (event_identifier)",
                fullyQualifiedName(schema, table),
                table);

        JdbcUtils.executeStatements(connection, createTable, createIndexAggidSeqnr, createIndexEventId);
    }

    private String fullyQualifiedName(String schema, String table) {
        if( schema == null) {
            return table;
        }
        return schema + "." + table;
    }

    @Override
    public void createSchemaIfNotExists(String schema, Connection connection) throws SQLException {
        try (ResultSet resultSet = connection.getMetaData().getSchemas(null, schema)) {
            if( resultSet.next()) {
                return;
            }
        }
        JdbcUtils.executeStatements(connection, "create schema " + schema);
    }

    @Override
    public void dropSchema(String schema, Connection connection) {
        try {
            JdbcUtils.executeStatements(connection, "drop schema " + schema + " cascade");
        } catch (SQLException sql) {
            System.out.println(sql.getErrorCode() + " - " + sql.getMessage());
        }
    }
}
