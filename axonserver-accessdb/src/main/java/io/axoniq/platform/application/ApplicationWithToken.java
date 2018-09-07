package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.Application;

/**
 * Author: marc
 */
public class ApplicationWithToken {
    private final String tokenString;
    private final Application application;

    public ApplicationWithToken(String tokenString, Application application) {
        this.tokenString = tokenString;
        this.application = application;
    }

    public String getTokenString() {
        return tokenString;
    }

    public Application getApplication() {
        return application;
    }
}
