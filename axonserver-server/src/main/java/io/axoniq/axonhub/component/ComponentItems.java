package io.axoniq.axonhub.component;

import java.util.Iterator;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 19/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ComponentItems<I extends ComponentItem> implements Iterable<I> {

    private final String component;

    private final String context;
    private final Iterable<I> delegate;

    public ComponentItems(String component, String context, Iterable<I> delegate) {
        this.component = component;
        this.context = context;
        this.delegate = delegate;
    }

    @Override
    public Iterator<I> iterator() {
        return stream(delegate.spliterator(), false)
                .filter(item -> item.belongsToComponent(component))
                .filter(item -> item.belongsToContext(context))
                .iterator();
    }
}
