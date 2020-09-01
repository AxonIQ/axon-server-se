package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.ClientIdentifications;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.version.BackwardsCompatibleVersion;
import io.axoniq.axonserver.component.version.ClientVersionsCache;
import io.axoniq.axonserver.component.version.UnknownVersion;
import io.axoniq.axonserver.component.version.Version;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static java.util.stream.StreamSupport.stream;

/**
 * {@link Clients} which support the heartbeat feature.
 *
 * @author Sara Pellegrini
 * @since 4.2.3
 */
@Component
public class HeartbeatProvidedClients implements ClientIdentifications {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatProvidedClients.class);

    private final List<Version> supportedAxonFrameworkVersions = asList(new BackwardsCompatibleVersion("4.2.1"),
                                                                        new BackwardsCompatibleVersion("4.3"),
                                                                        new BackwardsCompatibleVersion("5"));

    private final ClientIdentifications clients;

    private final Function<ClientStreamIdentification, Version> versionSupplier;

    /**
     * Constructs a {@link HeartbeatProvidedClients} starting from all clients and the {@link ClientVersionsCache} used
     * to retrieve the Axon Framework version for each of them.
     *
     * @param allClients    all the clients available
     * @param versionsCache the {@link ClientVersionsCache} used to retrieve the Axon Framework version of each client
     */
    @Autowired
    public HeartbeatProvidedClients(ClientIdentifications allClients, ClientVersionsCache versionsCache) {
        this(allClients,
             clientIdentification -> {
                 String version = versionsCache.apply(clientIdentification);
                 return (version == null || version.isEmpty()) ?
                         new UnknownVersion() : new BackwardsCompatibleVersion(version);
             });
    }

    /**
     * Constructs a {@link HeartbeatProvidedClients} starting from all clients and a function to retrieve
     * the Axon Framework version for each of them.
     *
     * @param allClients      all the clients available
     * @param versionSupplier the function used to retrieve the Axon Framework version of each client
     */
    public HeartbeatProvidedClients(ClientIdentifications allClients,
                                    Function<ClientStreamIdentification, Version> versionSupplier) {
        this.clients = allClients;
        this.versionSupplier = versionSupplier;
    }

    /**
     * Returns an iterator over {@link Client}s supporting the heartbeat feature.
     *
     * @return an iterator over {@link Client}s supporting the heartbeat feature.
     */
    @Nonnull
    @Override
    public Iterator<ClientStreamIdentification> iterator() {
        return stream(clients.spliterator(), false)
                .filter(this::supportHeartbeat)
                .iterator();
    }

    private boolean supportHeartbeat(ClientStreamIdentification clientIdentification) {
        Version clientVersion = versionSupplier.apply(clientIdentification);
        for (Version supportedVersion : supportedAxonFrameworkVersions) {
            try {
                if (clientVersion.greaterOrEqualThan(supportedVersion)) {
                    return true;
                }
            } catch (UnsupportedOperationException e1) {
                return false;
            } catch (Exception e2) {
                log.debug("Can not compare the client version with supported versions.", e2);
            }
        }
        return false;
    }
}
