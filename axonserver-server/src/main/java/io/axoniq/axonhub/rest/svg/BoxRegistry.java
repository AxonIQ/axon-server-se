package io.axoniq.axonhub.rest.svg;

import io.axoniq.axonhub.rest.svg.element.Box;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface BoxRegistry<K> {

    Box get(K key);

}
