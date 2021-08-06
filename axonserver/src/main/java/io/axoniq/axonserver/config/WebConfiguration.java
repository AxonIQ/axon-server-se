/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.serializer.Printable;
import io.axoniq.axonserver.serializer.PrintableSerializer;
import io.axoniq.axonserver.version.VersionInfoProvider;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.DefaultErrorAttributes;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;
import java.util.Map;

/**
 * Customizations for Spring MVC handlers.
 *
 * @author Marc Gathier
 */
@Configuration
public class WebConfiguration implements WebMvcConfigurer {

    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        configurer.setUseSuffixPatternMatch(false);
        configurer.setUseTrailingSlashMatch(false);
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/error").setViewName("error");
    }


    @Bean
    public ErrorAttributes errorAttributes(VersionInfoProvider versionInfoProvider) {
        return new DefaultErrorAttributes() {
            @Override
            public Map<String, Object> getErrorAttributes(WebRequest webRequest, ErrorAttributeOptions options) {
                Map<String, Object> errorAttributes = super.getErrorAttributes(webRequest, options);
                addMessageField(errorAttributes);
                errorAttributes.put("asVersion", versionInfoProvider.get().getVersion());
                errorAttributes.put("product", versionInfoProvider.get().getProductName());
                errorAttributes.remove("exception");
                errorAttributes.remove("errors");

                return errorAttributes;
            }

            private void addMessageField(Map<String, Object> errorAttributes) {
                @SuppressWarnings("unchecked")
                List<FieldError> errors = (List<FieldError>) errorAttributes.get("errors");
                if (errors != null && !errors.isEmpty()) {
                    errorAttributes.put("message", errors.get(0).getDefaultMessage());
                }
            }
        };
    }

    @ControllerAdvice
    public static class GlobalExceptionHandler {
        @ExceptionHandler({ IllegalArgumentException.class })
        @ResponseStatus( HttpStatus.BAD_REQUEST)
        public String onIllegalArgumentException(Throwable throwable) {
            return throwable.getMessage();
        }

        @ExceptionHandler({ IllegalAccessError.class })
        @ResponseStatus( HttpStatus.FORBIDDEN)
        public String onIllegalAccessException(Throwable throwable) {
            return throwable.getMessage();
        }

        @ExceptionHandler(value = {MessagingPlatformException.class})
        protected ResponseEntity<Object> handleAxonServerException(RuntimeException ex, WebRequest request) {
            MessagingPlatformException eventStoreException = (MessagingPlatformException)ex;
            return ResponseEntity.status(eventStoreException.getErrorCode().getHttpCode())
                    .header("AxonIQ-ErrorCode", eventStoreException.getErrorCode().getCode())
                    .body(ex.getMessage());
        }
    }

    @Bean
    public Jackson2ObjectMapperBuilder objectMapperBuilder() {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        builder.serializerByType(Printable.class, new PrintableSerializer());
        return builder;
    }

}
