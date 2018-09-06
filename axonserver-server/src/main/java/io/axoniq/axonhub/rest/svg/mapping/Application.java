package io.axoniq.axonhub.rest.svg.mapping;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface Application {

    String name();

    String component();

    String context();

    int instances();

    Iterable<String> connectedHubNodes();

    default String instancesString(){
        return instances() == 1 ? "1 instance" : instances() + " instances";
    }
}
