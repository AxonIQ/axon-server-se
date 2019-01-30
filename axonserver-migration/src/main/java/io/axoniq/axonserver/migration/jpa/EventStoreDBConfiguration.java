package io.axoniq.axonserver.migration.jpa;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.boot.orm.jpa.hibernate.SpringImplicitNamingStrategy;
import org.springframework.boot.orm.jpa.hibernate.SpringPhysicalNamingStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

/**
 * @author Marc Gathier
 */
@Configuration
@Profile({"migrate-from-jpa"})
@EnableJpaRepositories(basePackages = "io.axoniq.axonserver.migration.jpa",
        entityManagerFactoryRef = "eventStoreEntityManagerFactory")
public class EventStoreDBConfiguration {

    @Bean
    @Primary
    @Qualifier("eventStoreEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean eventStoreEntityManagerFactory(
            EntityManagerFactoryBuilder builder) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.physical_naming_strategy", SpringPhysicalNamingStrategy.class.getName());
        properties.put("hibernate.implicit_naming_strategy", SpringImplicitNamingStrategy.class.getName());
//        properties.put("hibernate.show_sql", "true");
//        properties.put("hibernate.format_sql", "true");

        return builder
                .dataSource(eventStoreDataSource())
                .packages("io.axoniq.axonserver.migration.jpa")
                .persistenceUnit("eventstore")
                .properties(properties)
                .build();
    }

    @Bean
    @Primary
    @ConfigurationProperties("axoniq.datasource.eventstore")
    public DataSourceProperties eventStoreDataSourceProperties() {
        return new DataSourceProperties();
    }


    @Bean
    @Primary
    @ConfigurationProperties("axoniq.datasource.eventstore")
public DataSource eventStoreDataSource() {
        return eventStoreDataSourceProperties().initializeDataSourceBuilder().build();
    }

}
