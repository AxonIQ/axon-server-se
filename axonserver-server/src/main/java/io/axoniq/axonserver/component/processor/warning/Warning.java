package io.axoniq.axonserver.component.processor.warning;

import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Created by Sara Pellegrini on 22/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface Warning extends Printable {

    boolean active();

    String message();

    @Override
    default void printOn(Media media) {
        media.with("message", message());
    }
}
