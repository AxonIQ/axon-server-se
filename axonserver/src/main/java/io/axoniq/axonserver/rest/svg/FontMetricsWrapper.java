package io.axoniq.axonserver.rest.svg;

import java.awt.*;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class FontMetricsWrapper {

    public static final FontMetricsWrapper INSTANCE = new FontMetricsWrapper(null);

    private final FontMetrics wrapped;

    public FontMetricsWrapper(FontMetrics wrapped) {
        this.wrapped = wrapped;
    }

    public int stringWidth(String text) {
        return wrapped == null ? text.length() * 10 : wrapped.stringWidth(text);
    }

    public int getHeight() {
        return wrapped == null ? 15 : wrapped.getHeight();
    }

    public int getAscent() {
        return wrapped == null ? 10 : wrapped.getAscent();
    }
}
