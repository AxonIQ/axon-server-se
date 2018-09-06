package io.axoniq.axonhub.rest.svg;

import io.axoniq.axonhub.rest.svg.attribute.Dimension;
import io.axoniq.axonhub.rest.svg.attribute.Position;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface Element extends Printable {

    Position position();

    Dimension dimension();

}
