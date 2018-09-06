package io.axoniq.sample;

/**
 * Author: marc
 */
public class EchoEvent {
    private String id;
    private String text;

    public EchoEvent() {

    }

    public EchoEvent(String id, String text) {
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
