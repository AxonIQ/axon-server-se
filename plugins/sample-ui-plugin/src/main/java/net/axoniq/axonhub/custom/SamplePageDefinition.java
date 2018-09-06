package net.axoniq.axonhub.custom;

import io.axoniq.axonhub.connector.Page;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@Configuration
public class SamplePageDefinition implements Page {
    @Override
    public String getUrl() {
        return "sample";
    }

    @Override
    public String getIconUrl() {
        return null;
    }

    @Bean
    public SampleRestController sampleRestController() {
        return new SampleRestController();
    }

    @Override
    public String getTitle() {
        return "Sample";
    }
}
