package io.axoniq.axonhub.component;

/**
 * Created by Sara Pellegrini on 30/03/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeComponentItem implements ComponentItem {

    private final Boolean belongs;

    public FakeComponentItem(Boolean belongs) {
        this.belongs = belongs;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return belongs;
    }

    @Override
    public boolean belongsToContext(String context) {
        return true;
    }
}
