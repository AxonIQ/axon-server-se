package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.enterprise.storage.jdbc.specific.PostgreSQLSpecific;
import org.junit.*;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Marc Gathier
 */
public class PostgreSQLSpecificTest {
    private PostgreSQLSpecific testSubject;
    private StorageProperties storageProperties;
    private Connection connection;

    @Before
    public void setUp() throws SQLException {
        testSubject = new PostgreSQLSpecific("second");
        storageProperties = new StorageProperties();
        storageProperties.setUrl("jdbc:postgresql://localhost/axondb");
        storageProperties.setDriver("org.postgresql.Driver");
        storageProperties.setUser("axonserver");
        storageProperties.setPassword("axonserver");
        connection = storageProperties.dataSource().getConnection();
        System.out.println(connection.getMetaData().getDatabaseProductName());
    }

    @After
    public void tearDown() throws SQLException {
        if( connection != null) connection.close();
    }

    @Test
    @Ignore
    public void createTableIfNotExists() throws SQLException {
        testSubject.createSchemaIfNotExists("DEMO_SCHEMA2", connection);
        testSubject.createTableIfNotExists("DEMO_SCHEMA2", "DEMO", connection);
        testSubject.createTableIfNotExists("DEMO_SCHEMA2", "DEMO", connection);
        testSubject.dropSchema("DEMO_SCHEMA2", connection);
    }

    @Test
    @Ignore
    public void createTableIfNotExistsInConnectionSchema() throws SQLException {
        testSubject.createTableIfNotExists( "DEMO", connection);
        testSubject.createTableIfNotExists("DEMO", connection);
    }

    @Test
    @Ignore
    public void createSchemaIfNotExists() throws SQLException {
        testSubject.createSchemaIfNotExists("DEMO_SCHEMA", connection);
        testSubject.createSchemaIfNotExists("DEMO_SCHEMA", connection);
        testSubject.dropSchema("DEMO_SCHEMA", connection);
    }
}