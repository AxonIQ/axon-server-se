package io.axoniq.axonserver.enterprise.storage.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Marc Gathier
 */
public class MySqlSpecific implements VendorSpecific {

    @Override
    public void createTableIfNotExists(String tableName, Connection connection) throws SQLException {
        createTableIfNotExists(null, tableName, connection);
    }

    @Override
    public void createTableIfNotExists(String schema, String table, Connection connection) throws SQLException {
        try (ResultSet resultSet = connection.getMetaData().getTables(schema, null, table, null)) {
            if (resultSet.next()) {
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


        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    createTable);
            try (Statement statement2 = connection.createStatement()) {
                statement2.execute(
                        createIndexAggidSeqnr);
            }
            try (Statement statement2 = connection.createStatement()) {
                statement2.execute(
                        createIndexEventId);
            }
        } catch (SQLException sql) {
            System.out.println(sql.getErrorCode() + " - " + sql.getMessage());
        }
    }

    private String fullyQualifiedName(String schema, String table) {
        if (schema == null) {
            return table;
        }
        return schema + "." + table;
    }

    @Override
    public void createSchemaIfNotExists(String schema, Connection connection) throws SQLException {
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            while (resultSet.next()) {
                if (schema.equalsIgnoreCase(resultSet.getString(1))) {
                    return;
                }
            }
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "create schema " + schema);
        } catch (SQLException sql) {
            System.out.println(sql.getErrorCode() + " - " + sql.getMessage());
        }
    }

    @Override
    public void dropSchema(String schema, Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "drop schema " + schema);
        } catch (SQLException sql) {
            System.out.println(sql.getErrorCode() + " - " + sql.getMessage());
        }
    }
}