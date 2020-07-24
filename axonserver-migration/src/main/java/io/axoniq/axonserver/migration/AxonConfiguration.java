package io.axoniq.axonserver.migration;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Marc Gathier
 */
@Configuration
public class AxonConfiguration {

    private final ApplicationContext applicationContext;

    public AxonConfiguration(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Bean
    public Serializer serializer(MigrationProperties migrationProperties) {
        if (SerializerType.JACKSON.equals(migrationProperties.getEvents())) {
            return JacksonSerializer.builder().build();
        }
        return XStreamSerializer.builder().build();
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
    public AxonServerConnectionManager axonServerConnectionManager(AxonServerConfiguration axonServerConfiguration) {
        return AxonServerConnectionManager.builder()
                                          .axonServerConfiguration(axonServerConfiguration)
                                          .build();
    }
}
