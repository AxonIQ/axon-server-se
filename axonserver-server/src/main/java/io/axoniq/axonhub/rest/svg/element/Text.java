package io.axoniq.axonhub.rest.svg.element;

import io.axoniq.axonhub.rest.svg.Printable;
import io.axoniq.axonhub.rest.svg.attribute.Position;
import io.axoniq.axonhub.rest.svg.attribute.StyleClass;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Text implements Printable {

    private final String text;

    private final Position position;

    private final StyleClass styleClass;

    public Text(String text, Position position, String styleClass) {
        this(text, position, new StyleClass(styleClass));
    }

    public Text(String text, Position position, StyleClass styleClass) {
        this.text = text;
        this.position = position;
        this.styleClass = styleClass;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("<text ");
        position.printOn(writer);
        styleClass.printOn(writer);
        writer.printf(">%s</text>\n", text);
    }

}
