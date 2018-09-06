package io.axoniq.axonhub.rest.svg;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface Mapping<T> {

    Element map(T item);

}
