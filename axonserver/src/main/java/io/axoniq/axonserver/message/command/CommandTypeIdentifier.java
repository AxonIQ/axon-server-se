package io.axoniq.axonserver.message.command;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Represents the key that allows to identify unequivocally a command type.
 *
 * @author Sara Pellegrini
 * @since 4.3
 */
public final class CommandTypeIdentifier {

    private final String context;

    private final String name;

    /**
     * Creates an instance with specified context and command name.
     *
     * @param context the bounded context of the command
     * @param name    the name of the command
     */
    public CommandTypeIdentifier(@Nonnull String context, @Nonnull String name) {
        this.context = context;
        this.name = name;
    }

    /**
     * Returns the bounded context of the command.
     *
     * @return the bounded context of the command.
     */
    public String context() {
        return context;
    }

    /**
     * Returns the name of the command.
     *
     * @return the name of the command.
     */
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommandTypeIdentifier that = (CommandTypeIdentifier) o;
        return context.equals(that.context) &&
                name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, name);
    }
}
