package io.axoniq.axonserver.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.Tag;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Specific configuration for Swagger pages.
 *
 * @author Marc Gathier
 */
@EnableSwagger2
@Profile("!production")
@Configuration
public class SwaggerConfiguration {
    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("io.axoniq"))
                .paths(PathSelectors.any())
                .build()
                .tags(new Tag("internal", "Unstable APIs, may change in next versions"));
    }

}
