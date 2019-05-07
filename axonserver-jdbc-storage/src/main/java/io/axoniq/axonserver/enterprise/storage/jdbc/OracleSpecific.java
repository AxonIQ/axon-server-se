package io.axoniq.axonserver.enterprise.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Marc Gathier
 */
public class OracleSpecific implements VendorSpecific {

    @Override
    public void createTableIfNotExists(String tableName, Connection connection) throws SQLException {
        createTableIfNotExists(connection.getMetaData().getUserName(), tableName, connection);
    }

    @Override
    public void createTableIfNotExists(String schema, String table, Connection connection) {
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

    @Override
    public void createSchemaIfNotExists(String schema, Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "create user " + schema + " identified by " + schema);
            try (Statement statement2 = connection.createStatement()) {
                statement2.execute(
                        "grant resource to " + schema);
            }
        } catch (SQLException sql) {
            System.out.println(sql.getErrorCode() + " - " + sql.getMessage());
        }
    }

    @Override
    public void dropSchema(String schema, Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "create drop " + schema + " cascade");
        } catch (SQLException sql) {
            System.out.println(sql.getErrorCode() + " - " + sql.getMessage());
        }

    }
}
