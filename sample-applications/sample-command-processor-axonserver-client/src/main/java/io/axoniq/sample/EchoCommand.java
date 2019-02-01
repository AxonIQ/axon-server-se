package io.axoniq.sample;


import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * @author Marc Gathier
 */
public class EchoCommand {
    @TargetAggregateIdentifier
    private String id;
    private String text;

    public EchoCommand() {
    }

    public EchoCommand(String id, String text) {
        this.id = id;
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public String getId() {
        return id;
    }
}
