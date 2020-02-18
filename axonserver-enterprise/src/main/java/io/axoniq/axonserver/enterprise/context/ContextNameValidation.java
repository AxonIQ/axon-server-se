package io.axoniq.axonserver.enterprise.context;

import java.util.function.Predicate;

/**
 * Validation function for the name of the context.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class ContextNameValidation implements Predicate<String> {

    /**
     * Checks if the context name is valid.
     * @param contextName the context name to be validate
     * @return true if the context name is acceptable, false otherwise
     */
    @Override
    public boolean test(String contextName) {
        return contextName.matches("[a-zA-Z][a-zA-Z@_\\-0-9]*");
    }
}
