package io.axoniq.axonserver.rest.svg.attribute;

import io.axoniq.axonserver.rest.svg.Printable;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Position implements Printable {

    private final int x;

    private final int y;

    public Position(int x, int y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.print("x=\""+x+"\" y=\""+y+"\" ");
    }

    public int x() {
        return x;
    }

    public int y() {
        return y;
    }

    public Position shift(int xOffset, int yOffset) {
        return new Position(x + xOffset, y+ yOffset);
    }

}
