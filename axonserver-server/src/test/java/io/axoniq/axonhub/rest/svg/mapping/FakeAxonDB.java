package io.axoniq.axonhub.rest.svg.mapping;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeAxonDB implements AxonDB {

    private final String name;
    private final Iterable<String> contexts;
    private final Iterable<String> connectedHubNodes;

    public FakeAxonDB(String name, Iterable<String> contexts,
                      Iterable<String> connectedHubNodes) {
        this.name = name;
        this.contexts = contexts;
        this.connectedHubNodes = connectedHubNodes;
    }


    @Override
    public String context() {
        return contexts.iterator().next();
    }



    @Override
    public boolean master() {
        return false;
    }
}
