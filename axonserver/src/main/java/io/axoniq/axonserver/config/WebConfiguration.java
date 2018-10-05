package io.axoniq.axonserver.config;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.rest.RestAuthenticationInterceptor;
import io.axoniq.axonserver.serializer.Printable;
import io.axoniq.axonserver.serializer.PrintableSerializer;
import io.axoniq.platform.KeepNames;
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
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;
import java.util.Map;

/**
 * @author Marc Gathier
 */
@Configuration
public class WebConfiguration implements WebMvcConfigurer {

    public WebConfiguration() {
    }

    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        configurer.setUseSuffixPatternMatch(false);
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/login").setViewName("login");
        registry.addViewController("/error").setViewName("error");
    }

    @Bean
    public ErrorAttributes errorAttributes() {
        return new DefaultErrorAttributes() {
            @Override
            public Map<String, Object> getErrorAttributes(WebRequest webRequest, boolean includeStackTrace) {
                Map<String, Object> errorAttributes =  super.getErrorAttributes(webRequest, includeStackTrace);
                addMessageField(errorAttributes);

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

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
    }


//    @Bean("mappingCustomizer")
//    public EndpointHandlerMappingCustomizer mappingCustomizer() {
//        if( configuration.getAccesscontrol() != null && configuration.getAccesscontrol().isEnabled()) {
//            return mapping -> mapping.setInterceptors(new RestAuthenticationInterceptor(axonHubAccessController));
//        }
//        return mapping -> {};
//    }

    @ControllerAdvice
    @KeepNames
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
        protected ResponseEntity<Object> handleEventStoreException(RuntimeException ex, WebRequest request) {
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
