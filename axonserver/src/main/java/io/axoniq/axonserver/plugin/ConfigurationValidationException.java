package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;

import java.util.Map;
import java.util.StringJoiner;

/**
 * Exceptions created inside AxonServer if the validation of configuration fails.
 *
 * @author Stefan Andjelkovic
 */
public class ConfigurationValidationException extends MessagingPlatformException {
    private Map<String, Iterable<ConfigurationError>> errors;

    public ConfigurationValidationException(Map<String, Iterable<ConfigurationError>> errors) {
        super(ErrorCode.INVALID_PLUGIN_CONFIGURATION, errors.toString());
        this.errors = errors;
    }

    @Override
    public Object getErrorPayload() {
        return errors;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ConfigurationValidationException.class.getSimpleName() + "[", "]")
                .add("errors=" + errors)
                .toString();
    }

}
