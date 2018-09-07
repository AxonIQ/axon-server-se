package io.axoniq.axonserver.component.instance;

/**
 * Created by Sara Pellegrini on 30/03/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeClient implements Client {

    private final String name;

    private final String context;

    private final boolean belongs;

    public FakeClient(String name, boolean belongs) {
        this(name, "no context", belongs);
    }

    public FakeClient(String name, String context, boolean belongs) {
        this.name = name;
        this.context = context;
        this.belongs = belongs;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return belongs;
    }
}
