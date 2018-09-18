package io.axoniq.axonserver.rest.svg.element;

import io.axoniq.axonserver.rest.svg.Element;
import io.axoniq.axonserver.rest.svg.Printable;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public interface Box extends Printable, Element {

    Rectangle rectangle();

    void connectTo(Box connected, String lineStyle);

}
