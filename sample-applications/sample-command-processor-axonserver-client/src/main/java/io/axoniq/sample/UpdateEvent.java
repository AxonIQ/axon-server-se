package io.axoniq.sample;

/**
 * @author Marc Gathier
 */
public class UpdateEvent {
    private String id;
    private String text;

    public UpdateEvent() {
    }

    public UpdateEvent(String id, String text) {
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
