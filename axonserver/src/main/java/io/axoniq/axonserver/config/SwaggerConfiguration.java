/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

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
//@EnableSwagger2
@Profile("!production")
@Configuration
public class SwaggerConfiguration {
//    @Bean
//    public Docket api() {
//        return new Docket(DocumentationType.SWAGGER_2)
//                .select()
//                .apis(RequestHandlerSelectors.basePackage("io.axoniq"))
//                .paths(PathSelectors.any())
//                .build()
//                .tags(new Tag("internal", "Unstable APIs, may change in next versions"));
//    }

    @Bean
    public GroupedOpenApi publicApi() {
        return GroupedOpenApi.builder()
                             .group("Axon Server API")
                             .pathsToMatch("/v1/**")
                             .build();
    }

    @Bean
    public GroupedOpenApi internal() {
        return GroupedOpenApi.builder()
                             .group("Axon Server Internal API")
                             .pathsToMatch("/internal/**")
                             .build();
    }

    @Bean
    public OpenAPI springShopOpenAPI() {
        return new OpenAPI()
                .info(new Info().title("Axon Server API")
                                .description(
                                        "API consists of 2 groups, the public API and the internal API. Operations in the internal API are not guaranteed to be supported in subsequent versions of Axon Server")
                                .version("v4.6.0"));
    }
}
