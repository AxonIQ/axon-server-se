package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;

public interface CommandRequest extends Serializable {

    Command command();

}
