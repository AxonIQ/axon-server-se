package io.axoniq.axonhub.component.instance;

import java.util.Iterator;
import javax.annotation.Nonnull;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 16/05/2018.
 * sara.pellegrini@gmail.com
 */
public class ClientsNames implements Iterable<String> {

    private final Iterable<Client> clients;

    public ClientsNames(Iterable<Client> clients) {
        this.clients = clients;
    }

    @Override @Nonnull
    public Iterator<String> iterator() {
        return stream(clients.spliterator(),false).map(Client::name).iterator();
    }
}
