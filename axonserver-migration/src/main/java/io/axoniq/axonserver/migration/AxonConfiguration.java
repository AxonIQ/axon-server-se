package io.axoniq.axonserver.migration;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.PlatformConnectionManager;
import org.axonframework.axonserver.connector.event.AxonDBClient;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@Configuration
public class AxonConfiguration implements ApplicationContextAware {
    private ApplicationContext applicationContext;

    @Bean
    public Serializer serializer(MigrationProperties migrationProperties) {
        if( SerializerType.JACKSON.equals(migrationProperties.getEvents() ) ) {
            return new JacksonSerializer();
        }
        return new XStreamSerializer();
    }


    @Bean
    public AxonServerConfiguration axonServerConfiguration() {
        AxonServerConfiguration configuration = new AxonServerConfiguration();
        configuration.setComponentName(clientName(applicationContext.getId()));
        return configuration;
    }

    private String clientName(String id) {
        if( id == null) return "AxonServerMigration";
        if (id.contains(":")) return id.substring(0, id.indexOf(':'));
        return id;
    }

    @Bean
    public AxonDBClient axonDBClient(AxonServerConfiguration axonServerConfiguration,
            PlatformConnectionManager platformConnectionManager
            ) {
        return new AxonDBClient(axonServerConfiguration, platformConnectionManager);

    }

    @Bean
    public PlatformConnectionManager platformConnectionManager(AxonServerConfiguration routingConfiguration) {
        return new PlatformConnectionManager(routingConfiguration);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }
}
