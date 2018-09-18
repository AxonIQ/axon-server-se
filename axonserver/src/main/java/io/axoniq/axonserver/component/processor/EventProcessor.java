package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.warning.ActiveWarnings;
import io.axoniq.axonserver.component.processor.warning.Warning;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

import static java.util.Collections.emptyList;

/**
 * Created by Sara Pellegrini on 13/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface EventProcessor extends Printable {

    String name();

    String mode();

    default Iterable<Warning> warnings(){
        return emptyList();
    }

    @Override
    default void printOn(Media media){
        media.with("name", name())
             .with("mode", mode())
             .with("warnings", new ActiveWarnings(warnings()));
    }
}
