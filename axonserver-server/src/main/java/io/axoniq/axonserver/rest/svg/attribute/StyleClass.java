package io.axoniq.axonserver.rest.svg.attribute;

import io.axoniq.axonserver.rest.svg.Printable;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class StyleClass implements Printable {

    private final String styleClass;

    public StyleClass(String styleClass) {
        this.styleClass = styleClass;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("class=\"%s\"",styleClass);
    }

    @Override
    public String toString() {
        return styleClass;
    }
}
