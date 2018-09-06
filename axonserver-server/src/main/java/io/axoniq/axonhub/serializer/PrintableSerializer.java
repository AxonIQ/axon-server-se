package io.axoniq.axonhub.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * Created by Sara Pellegrini on 13/03/2018.
 * sara.pellegrini@gmail.com
 */
public class PrintableSerializer extends StdSerializer<Printable> {

    public PrintableSerializer() {
        super(Printable.class);
    }

    @Override
    public void serialize(Printable value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        Media media = new GsonMedia();
        value.printOn(media);
        gen.writeRawValue(media.toString());
    }
}
