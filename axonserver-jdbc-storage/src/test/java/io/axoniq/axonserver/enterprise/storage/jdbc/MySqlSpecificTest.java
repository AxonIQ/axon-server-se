package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.enterprise.storage.jdbc.specific.MySqlSpecific;
import org.junit.*;

import java.sql.Connection;

/**
 * @author Marc Gathier
 */
public class MySqlSpecificTest {
    private MySqlSpecific testSubject;
    private StorageProperties storageProperties;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        testSubject = new MySqlSpecific();
        storageProperties = new StorageProperties();
        storageProperties.setUrl("jdbc:mysql://localhost:3306/?serverTimezone=UTC");
        storageProperties.setDriver("com.mysql.cj.jdbc.Driver");
        storageProperties.setUser("axonserver2");
        storageProperties.setPassword("axonserver2");
        connection = storageProperties.dataSource().getConnection();
    }

    @After
    public void tearDown() throws Exception {
        if( connection != null) connection.close();
    }

    @Test
    @Ignore
    public void createTableIfNotExists() throws Exception {
        testSubject.createSchemaIfNotExists("DEMO_SCHEMA2", connection);
        testSubject.createTableIfNotExists("DEMO_SCHEMA2", "DEMO", connection);
        testSubject.createTableIfNotExists("DEMO_SCHEMA2", "DEMO", connection);
        testSubject.dropSchema("DEMO_SCHEMA2", connection);
    }

    @Test
    @Ignore
    public void createSchemaIfNotExists() throws Exception {
        testSubject.createSchemaIfNotExists("DEMO_SCHEMA", connection);
        testSubject.createSchemaIfNotExists("DEMO_SCHEMA", connection);
        testSubject.dropSchema("DEMO_SCHEMA", connection);
    }

    @Test
    @Ignore
    public void dropSchema() {
    }
}