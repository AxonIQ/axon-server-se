package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import org.junit.*;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Marc Gathier
 */
public class SchemaPerContextMultiContextStrategyTest {

    private SchemaPerContextMultiContextStrategy testSubject;
    private Connection connection;
    private StorageProperties storageProperties;

    @Before
    public void setUp() throws Exception {
        storageProperties = new StorageProperties();
//        storageProperties.setDriver("com.mysql.jdbc.Driver");
//        storageProperties.setUrl("jdbc:mysql://localhost:3306/?serverTimezone=UTC");
//        storageProperties.setUser("axonserver2");
//        storageProperties.setPassword("axonserver2");
//        storageProperties.setUrl("jdbc:oracle:thin:@localhost:1521:XE");
//        storageProperties.setDriver("oracle.jdbc.driver.OracleDriver");
//        storageProperties.setUser("axonserver2");
//        storageProperties.setPassword("axonserver2");
        connection = storageProperties.dataSource().getConnection();
        testSubject = new SchemaPerContextMultiContextStrategy(storageProperties.getVendorSpecific());
    }

    @After
    public void tearDown() throws Exception {
        storageProperties.getVendorSpecific().dropSchema(testSubject.schema("test"), connection);
    }

    @Test
    public void init() throws SQLException {
        testSubject.init(new EventTypeContext("default", EventType.EVENT), connection);
    }

    @Test
    public void initWithSchemaExisting() throws SQLException {
        storageProperties.getVendorSpecific().createSchemaIfNotExists(testSubject.schema("test"), connection);
        testSubject.init(new EventTypeContext("test", EventType.EVENT), connection);
    }
}