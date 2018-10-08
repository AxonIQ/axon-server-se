package io.axoniq.axonserver.rest.svg.attribute;

import io.axoniq.axonserver.rest.svg.Printable;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class StyleClass implements Printable {
    public static final String POPUP = "popup";
    public static final String SERVER_TO_SERVER = "hubToHub";
    public static final String AXONSERVER = "axonserver";
    public static final String TYPE = "type";
    public static final String DOWN = "down";
    public static final String CURRENT = "current";
    public static final String STORAGE = "axondb";
    public static final String MASTER = "master";
    public static final String CLIENT = "client";

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
