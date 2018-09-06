package io.axoniq.axonhub.rest.svg;

import io.axoniq.axonhub.rest.svg.attribute.Position;

/**
 * Created by Sara Pellegrini on 30/04/2018.
 * sara.pellegrini@gmail.com
 */
public interface PositionMapping<T> {

    Element map(T item, Position position);

}
