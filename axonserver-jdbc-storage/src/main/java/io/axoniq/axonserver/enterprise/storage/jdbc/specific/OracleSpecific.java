package io.axoniq.axonserver.enterprise.storage.jdbc.specific;

import io.axoniq.axonserver.enterprise.storage.jdbc.JdbcUtils;
import io.axoniq.axonserver.enterprise.storage.jdbc.VendorSpecific;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Creation and deletion of database objects specific for an Oracle database. Oracle does not support create schema command,
 * so for multi-context option schema-per-context it creates a user per context.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class OracleSpecific implements VendorSpecific {

    @Override
    public void createTableIfNotExists(String tableName, Connection connection) throws SQLException {
        createTableIfNotExists(connection.getMetaData().getUserName(), tableName, connection);
    }

    @Override
    public void createTableIfNotExists(String schema, String table, Connection connection) throws SQLException {
        String createTable = String.format(
                "create table %s.%s ("
                        + "global_index int not null, "
                        + "aggregate_identifier varchar(255) not null, "
                        + "event_identifier varchar(255) not null, "
                        + "meta_data blob, "
                        + "payload blob not null, "
                        + "payload_revision varchar(255), "
                        + "payload_type varchar(255) not null, "
                        + "sequence_number int not null, "
                        + "time_stamp int not null, "
                        + "type varchar(255), "
                        + "primary key (global_index))", schema, table);
        String createIndexAggidSeqnr = String.format(
                "alter table %s.%s add constraint %s_uk1 unique (aggregate_identifier, sequence_number)", schema, table, table);
        String createIndexEventId = String.format(
                "alter table %s.%s add constraint %s_uk2 unique (event_identifier)", schema, table, table);

        JdbcUtils.executeStatements(connection, createTable, createIndexAggidSeqnr, createIndexEventId);
    }

    @Override
    public void createSchemaIfNotExists(String schema, Connection connection) throws SQLException {
        JdbcUtils.executeStatements(connection, "create user " + schema + " identified by " + schema,
                                    "grant resource to " + schema);
    }

    @Override
    public void dropSchema(String schema, Connection connection) {
        try {
            JdbcUtils.executeStatements(connection,
                                    "create drop " + schema + " cascade");
        } catch (SQLException sql) {
            System.out.println(sql.getErrorCode() + " - " + sql.getMessage());
        }
    }

    @Override
    public boolean tableExists(String schema, String table, Connection connection) throws SQLException {
        return true;
    }

    @Override
    public boolean tableExists(String tableName, Connection connection) throws SQLException {
        return true;
    }
}
