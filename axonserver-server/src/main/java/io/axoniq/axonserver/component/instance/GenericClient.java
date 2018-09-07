package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.serializer.Media;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class GenericClient implements Client {

    private final String clientId;

    private final String componentName;

    private final String context;
    private final String axonHubServer;

    public GenericClient(String clientId, String componentName, String context, String axonHubServer) {
        this.clientId = clientId;
        this.componentName = componentName;
        this.context = context;
        this.axonHubServer = axonHubServer;
    }

    @Override
    public String name() {
        return clientId;
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return component.equals(componentName);
    }

    @Override
    public void printOn(Media media) {
        media.with("name", name()).with("componentName", componentName).with("axonHubServer", axonHubServer);
    }
}
