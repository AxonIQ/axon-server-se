package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.version.BackwardCompatibilityVersion;
import io.axoniq.axonserver.component.version.ClientVersionsCache;
import io.axoniq.axonserver.component.version.UnknownVersion;
import io.axoniq.axonserver.component.version.Version;
import io.axoniq.axonserver.message.ClientIdentification;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.stream.StreamSupport.stream;

/**
 * @author Sara Pellegrini
 * @since 4.2.3
 */
@Component
public class HeartbeatProvidedClients implements Clients {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatProvidedClients.class);

    private final List<String> supportedAxonFrameworkVersions = asList("4.2.1", "4.3", "5");

    private final Clients connectedClients;

    private final Function<ClientIdentification, Version> versionSupplier;

    @Autowired
    public HeartbeatProvidedClients(Clients allClients, ClientVersionsCache versionsCache) {
        this(allClients,
             clientIdentification -> {
                 String version = versionsCache.apply(clientIdentification);
                 return (version == null || version.isEmpty()) ?
                         new UnknownVersion() : new BackwardCompatibilityVersion(version);
             });
    }

    public HeartbeatProvidedClients(Clients connectedClients,
                                    Function<ClientIdentification, Version> versionSupplier) {
        this.connectedClients = connectedClients;
        this.versionSupplier = versionSupplier;
    }

    @NotNull
    @Override
    public Iterator<Client> iterator() {
        return stream(connectedClients.spliterator(), false)
                .filter(this::supportHeartbeat)
                .iterator();
    }

    private boolean supportHeartbeat(Client client) {
        Version clientVersion = versionSupplier.apply(new ClientIdentification(client.context(), client.name()));
        for (String supportedVersionClass : supportedAxonFrameworkVersions) {
            try {
                if (clientVersion.greaterOrEqualThan(supportedVersionClass)) {
                    return true;
                }
            } catch (UnsupportedOperationException e1) {
                return false;
            } catch (Exception e2) {
                log.debug("Impossible to compare the client version with supported versions.", e2);
            }
        }
        return false;
    }
}
