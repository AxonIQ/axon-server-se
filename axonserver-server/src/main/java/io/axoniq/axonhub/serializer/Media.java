package io.axoniq.axonhub.serializer;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface Media {

    Media with(String property, String value);

    Media with(String property, Number value);

    Media with(String property, Boolean value);

    Media with(String property, Printable value);

    Media with(String property, Iterable<? extends Printable> values);

    Media withStrings(String property, Iterable<String> values);

    String toString();

}
