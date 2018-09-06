package io.axoniq.axonhub.rest.svg;

import io.axoniq.axonhub.rest.svg.attribute.Dimension;
import io.axoniq.axonhub.rest.svg.attribute.Position;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface Composite extends Element {

    Iterable<? extends Element> items();

    @Override
    default Position position() {
        int x = stream(items().spliterator(), false)
                .map(Element::position)
                .map(Position::x)
                .min(Integer::compareTo).orElse(0);
        int y = stream(items().spliterator(), false)
                .map(Element::position)
                .map(Position::y)
                .min(Integer::compareTo).orElse(0);
        return new Position(x, y);
    }

    @Override
    default Dimension dimension() {
        Integer lastX = stream(items().spliterator(), false)
                .map(element -> element.position().x() + element.dimension().width())
                .max(Integer::compareTo)
                .orElse(0);

        Integer lastY = stream(items().spliterator(), false)
                .map(element -> element.position().y() + element.dimension().height())
                .max(Integer::compareTo)
                .orElse(0);
        Position p = position();
        return new Dimension(lastX - p.x(), lastY - p.y());
    }
}
