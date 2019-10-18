package io.axoniq.axonserver.migration.db;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.boot.orm.jpa.hibernate.SpringImplicitNamingStrategy;
import org.springframework.boot.orm.jpa.hibernate.SpringPhysicalNamingStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.HashMap;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

/**
 * @author Marc Gathier
 */
@Configuration
@EnableJpaRepositories(basePackages = "io.axoniq.axonserver.migration.db",
        entityManagerFactoryRef = "migrationEntityManagerFactory")
@EnableTransactionManagement
public class    MigrationDBConfiguration {

    @Bean
    @Qualifier("migrationEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean migrationEntityManagerFactory(
            EntityManagerFactoryBuilder builder) {
        Map<String, String> properties = new HashMap<>();
        properties.put("hibernate.hbm2ddl.auto", "update");
        properties.put("hibernate.physical_naming_strategy", SpringPhysicalNamingStrategy.class.getName());
        properties.put("hibernate.implicit_naming_strategy", SpringImplicitNamingStrategy.class.getName());
//        properties.put("hibernate.show_sql", "true");
//        properties.put("hibernate.format_sql", "true");
        return builder
                .dataSource(migrationDataSource())
                .packages("io.axoniq.axonserver.migration.db")
                .persistenceUnit("migration")
                .properties(properties)
                .build();
    }


    @Bean
    @ConfigurationProperties("axoniq.datasource.migration")
    public DataSourceProperties migrationDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("axoniq.datasource.migration")
    public DataSource migrationDataSource() {
        return migrationDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean(name = "transactionManager")
    public PlatformTransactionManager readingTransactionManager(
            @Qualifier("migrationEntityManagerFactory") EntityManagerFactory migrationEntityManagerFactory) {
        return new JpaTransactionManager(migrationEntityManagerFactory);
    }
}
