package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.grpc.internal.NodeMetrics;

/**
 * Created by Sara Pellegrini on 11/04/2018.
 * sara.pellegrini@gmail.com
 */
public class MetricsEvents {

    @KeepNames
    public static class MetricsChanged {

        private final NodeMetrics nodeMetrics;

        public MetricsChanged(NodeMetrics nodeMetrics) {
            this.nodeMetrics = nodeMetrics;
        }

        public NodeMetrics nodeMetrics() {
            return nodeMetrics;
        }
    }
}
