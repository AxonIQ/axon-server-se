package io.axoniq.axonhub.rest.svg.decorator;


import io.axoniq.axonhub.rest.svg.Element;
import io.axoniq.axonhub.rest.svg.Elements;
import io.axoniq.axonhub.rest.svg.attribute.Dimension;
import io.axoniq.axonhub.rest.svg.attribute.Position;
import io.axoniq.axonhub.rest.svg.attribute.StyleClass;
import io.axoniq.axonhub.rest.svg.element.Rectangle;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public class Grouped implements Element {

    private final Rectangle rectangle;

    private final Elements content;

    public Grouped(Elements content, String styleClass) {
        this.content = content;
        this.rectangle = new Rectangle(content.position().shift(-5,-5),
                                       content.dimension().increase(15, 15),
                                       new StyleClass(styleClass));
    }

    @Override
    public void printOn(PrintWriter writer) {
        if( content.boxesNumber() > 1) {
            rectangle.printOn(writer);
        }
        content.printOn(writer);
    }

    @Override
    public Position position() {
        return rectangle.position();
    }

    @Override
    public Dimension dimension() {
        return rectangle.dimension();
    }
}
