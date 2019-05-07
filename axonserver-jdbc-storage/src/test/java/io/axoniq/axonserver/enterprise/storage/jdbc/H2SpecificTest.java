package io.axoniq.axonserver.enterprise.storage.jdbc;

import org.junit.*;

import java.sql.Connection;

/**
 * @author Marc Gathier
 */
public class H2SpecificTest {
    private H2Specific testSubject;
    private StorageProperties storageProperties;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        testSubject = new H2Specific();
        storageProperties = new StorageProperties();
        connection = storageProperties.dataSource().getConnection();
    }

    @After
    public void tearDown() throws Exception {
        if( connection != null) connection.close();
    }

    @Test
    public void createTableIfNotExists() throws Exception {
        testSubject.createTableIfNotExists("DEMO", connection);
        testSubject.createTableIfNotExists("DEMO", connection);
    }

    @Test
    public void createSchemaIfNotExists() throws Exception {
        testSubject.createSchemaIfNotExists("DEMO_SCHEMA", connection);
        testSubject.createSchemaIfNotExists("DEMO_SCHEMA", connection);
    }

    @Test
    public void dropSchema() {
    }
}