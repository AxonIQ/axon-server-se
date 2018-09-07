package io.axoniq.axonserver.rest.svg;

import io.axoniq.axonserver.rest.svg.element.Box;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface BoxRegistry<K> {

    Box get(K key);

}
