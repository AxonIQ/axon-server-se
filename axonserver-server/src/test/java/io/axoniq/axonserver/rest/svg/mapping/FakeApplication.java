package io.axoniq.axonserver.rest.svg.mapping;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeApplication implements Application {

    private final String name;
    private final String component;
    private final String context;
    private final int instances;
    private final Iterable<String> connectedhubNodes;

    public FakeApplication(String name, String component, String context, int instances,
                           Iterable<String> connectedhubNodes) {
        this.name = name;
        this.component = component;
        this.context = context;
        this.instances = instances;
        this.connectedhubNodes = connectedhubNodes;
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public String component() {
        return component;
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public int instances() {
        return instances;
    }

    @Override
    public Iterable<String> connectedHubNodes() {
        return connectedhubNodes;
    }
}
