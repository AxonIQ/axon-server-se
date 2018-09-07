package io.axoniq.axonserver.rest.svg.attribute;

import io.axoniq.axonserver.rest.svg.Printable;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Dimension implements Printable {

    private final int width;

    private final int height;

    public Dimension(int width, int height) {
        this.width = width;
        this.height = height;
    }

    public Dimension increase(int width, int height){
        return new Dimension(this.width + width, this.height + height);
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.print("width=\""+width+"\" height=\""+height+"\" ");
    }

    public int width() {
        return width;
    }

    public int height() {
        return height;
    }
}
