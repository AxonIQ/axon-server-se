package io.axoniq.axonhub.component.processor.warning;

import java.util.Iterator;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 22/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ActiveWarnings implements Iterable<Warning> {

    private final Iterable<Warning> warnings;

    public ActiveWarnings(Iterable<Warning> warnings) {
        this.warnings = warnings;
    }

    @Override
    public Iterator<Warning> iterator() {
        return stream(warnings.spliterator(), false)
                .filter(Warning::active)
                .iterator();
    }
}
