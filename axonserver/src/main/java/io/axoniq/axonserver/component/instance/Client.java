package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.component.ComponentItem;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface Client extends Printable, ComponentItem {

    String name();

    String context();

    @Override
    default boolean belongsToContext(String context){
        return context.equals(context());
    }

    @Override
    default void printOn(Media media) {
        media.with("name", name());
    }
}
