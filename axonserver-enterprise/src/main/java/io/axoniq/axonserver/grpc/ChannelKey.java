package io.axoniq.axonserver.grpc;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
final class ChannelKey {

    @Nonnull private final String host;
    private final int port;

    ChannelKey(@Nonnull String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChannelKey that = (ChannelKey) o;
        return port == that.port &&
                host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }
}
