package io.axoniq.axonserver.access;

public class ApplicationBinding {

    private final String name;

    public ApplicationBinding(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }
}
