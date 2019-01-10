package io.axoniq.axonserver.message;

import java.util.Comparator;
import java.util.Objects;

/**
 * Author: marc
 */
public class ClientIdentification implements Comparable<ClientIdentification> {
    private static final Comparator<ClientIdentification> COMPARATOR = Comparator.comparing(ClientIdentification::getContext).thenComparing(ClientIdentification::getClient);
    private final String context;
    private final String client;


    public ClientIdentification(String context, String client) {
        this.context = context;
        this.client = client;
    }

    public String getContext() {
        return context;
    }

    public String getClient() {
        return client;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientIdentification that = (ClientIdentification) o;
        return Objects.equals(context, that.context) &&
                Objects.equals(client, that.client);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, client);
    }

    public int compareTo(ClientIdentification client) {
        return COMPARATOR.compare(this, client);
    }

    @Override
    public String toString() {
        return context + "/" + client;
    }
}
