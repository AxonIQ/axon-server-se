package io.axoniq.axonserver.component.processor.warning;

/**
 * Created by Sara Pellegrini on 30/03/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeWarning implements Warning {

    private final boolean active;

    private final String message;

    public FakeWarning(boolean active, String message) {
        this.active = active;
        this.message = message;
    }

    @Override
    public boolean active() {
        return active;
    }

    @Override
    public String message() {
        return message;
    }
}
