package io.axoniq.axonserver;

import io.axoniq.axonserver.internal.grpc.NodeMetrics;

/**
 * Created by Sara Pellegrini on 11/04/2018.
 * sara.pellegrini@gmail.com
 */
public class MetricsEvents {

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
