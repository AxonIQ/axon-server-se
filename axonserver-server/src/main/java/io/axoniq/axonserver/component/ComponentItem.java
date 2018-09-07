package io.axoniq.axonserver.component;

/**
 * Created by Sara Pellegrini on 19/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface ComponentItem {

    Boolean belongsToComponent(String component);

    boolean belongsToContext(String context);
}
