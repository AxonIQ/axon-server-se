package io.axoniq.sample;

import org.axonframework.commandhandling.TargetAggregateIdentifier;

/**
 * Author: marc
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

    public void setId(String id) {
        this.id = id;
    }

    public void setText(String text) {
        this.text = text;
    }

}
