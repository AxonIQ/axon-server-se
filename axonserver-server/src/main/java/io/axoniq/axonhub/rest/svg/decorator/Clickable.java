package io.axoniq.axonhub.rest.svg.decorator;

import io.axoniq.axonhub.rest.svg.Element;
import io.axoniq.axonhub.rest.svg.attribute.Dimension;
import io.axoniq.axonhub.rest.svg.attribute.Position;

import java.io.PrintWriter;
import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Clickable implements Element {

    private final Element content;

    private final Supplier<String> onClickJavascript;

    public Clickable(Element content, Supplier<String> onClickJavascript) {
        this.onClickJavascript = onClickJavascript;
        this.content = content;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("<g onclick=\"%s\">%n", onClickJavascript.get());
        content.printOn(writer);
        writer.printf("</g>%n");
    }

    @Override
    public Position position() {
        return content.position();
    }

    @Override
    public Dimension dimension() {
        return content.dimension();
    }
}
