/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.version.VersionInfoProvider;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springdoc.core.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Specific configuration for Swagger pages.
 *
 * @author Marc Gathier
 */
@Profile("!production")
@Configuration
public class SwaggerConfiguration {

    /**
     * Returns the {@link GroupedOpenApi} definitions for the public services.
     *
     * @return the {@link GroupedOpenApi} definitions for the public services
     */
    @Bean
    public GroupedOpenApi publicApi() {
        return GroupedOpenApi.builder()
                             .displayName("Axon Server API")
                             .group("api")
                             .pathsToMatch("/v1/**")
                             .build();
    }

    /**
     * Creates an {@link OpenAPI} bean, with customized information for Axon Server.
     *
     * @param versionInfoProvider provides version information
     * @return the {@link OpenAPI} bean.
     */
    @Bean
    public OpenAPI axonServerOpenAPI(VersionInfoProvider versionInfoProvider) {
        return new OpenAPI()
                .info(new Info().title(versionInfoProvider.get().getProductName() + " API")
                                .description("API for Axon Server http services.")
                                .version(versionInfoProvider.get().getVersion()));
    }
}
