package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.enterprise.storage.jdbc.specific.H2Specific;
import io.axoniq.axonserver.enterprise.storage.jdbc.specific.MySqlSpecific;
import io.axoniq.axonserver.enterprise.storage.jdbc.specific.OracleSpecific;
import io.axoniq.axonserver.enterprise.storage.jdbc.specific.PostgreSQLSpecific;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Specific properties for RDMBS storage.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@ConfigurationProperties(prefix = "axoniq.axonserver.storage.jdbc")
@Configuration
public class StorageProperties {

    /**
     * The URL for the database connection
     */
    private String url = "jdbc:h2:mem:test_mem";
    /**
     * The database driver to use
     */
    private String driver = "org.h2.Driver";
    /**
     * THe username to logon to the database
     */
    private String user;
    /**
     * The password to logon to the database
     */
    private String password;

    private String defaultSchema;

    private VendorSpecific vendorSpecific;

    /**
     * The strategy to apply for multi-context
     */
    private MultiContextOption multiContextStrategy = MultiContextOption.SINGLE_SCHEMA;
    /**
     * Store events only when current node is leader
     */
    private boolean storeOnLeaderOnly;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public DataSource dataSource() {
        return DataSourceBuilder.create()
                                .driverClassName(driver)
                                .username(user)
                                .password(password)
                                .url(url)
                                .build();
    }

    public MultiContextOption getMultiContextStrategy() {
        return multiContextStrategy;
    }

    public void setMultiContextStrategy(MultiContextOption multiContextStrategy) {
        this.multiContextStrategy = multiContextStrategy;
    }

    public VendorSpecific getVendorSpecific() throws SQLException {
        if (vendorSpecific == null) {
            synchronized (this) {
                if (vendorSpecific == null) initVendorSpecific();
            }
        }
        return vendorSpecific;
    }

    private void initVendorSpecific() throws SQLException {
        try (Connection connection = dataSource().getConnection() ){
            switch( connection.getMetaData().getDatabaseProductName().toUpperCase()) {
                case "ORACLE":
                    vendorSpecific = new OracleSpecific();
                    break;
                case "MYSQL":
                    vendorSpecific = new MySqlSpecific();
                    break;
                case "POSTGRESQL":
                    vendorSpecific = new PostgreSQLSpecific(defaultSchema);
                    break;
                case "H2":
                default:
                    vendorSpecific = new H2Specific();
                    break;
            }

        }
    }

    public boolean isStoreOnLeaderOnly() {
        return storeOnLeaderOnly;
    }

    public void setStoreOnLeaderOnly(boolean storeOnLeaderOnly) {
        this.storeOnLeaderOnly = storeOnLeaderOnly;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }
}
