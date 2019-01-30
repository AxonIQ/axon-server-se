package io.axoniq.axonserver.enterprise.storage.jdbc;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * @author Marc Gathier
 */
@ConfigurationProperties(prefix = "axoniq.axonserver.storage.jdbc")
@Configuration
public class StorageProperties {
    private String url = "jdbc:h2:mem:test_mem";
    private String driver = "org.h2.Driver";
    private String user;
    private String password;

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
}
