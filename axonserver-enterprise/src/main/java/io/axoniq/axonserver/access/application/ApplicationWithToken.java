package io.axoniq.axonserver.access.application;


import io.axoniq.axonserver.access.jpa.Application;

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
