package io.axoniq.axonhub;

import io.axoniq.axonhub.internal.grpc.LoadBalancingStrategies;
import io.axoniq.axonhub.internal.grpc.ProcessorsLBStrategy;
import io.axoniq.platform.grpc.LoadBalanceStrategy;
import io.axoniq.platform.grpc.ProcessorLBStrategy;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
public class LoadBalancingSynchronizationEvents {

    @KeepNames
    public static class LoadBalancingStrategiesReceived {
        private final LoadBalancingStrategies strategies;

        public LoadBalancingStrategiesReceived(LoadBalancingStrategies strategies) {
            this.strategies = strategies;
        }

        public LoadBalancingStrategies strategies() {
            return strategies;
        }
    }

    @KeepNames
    public static class LoadBalancingStrategyReceived {
        private final LoadBalanceStrategy strategy;
        private final boolean proxied;

        public LoadBalancingStrategyReceived(LoadBalanceStrategy strategy, boolean proxied) {
            this.strategy = strategy;
            this.proxied = proxied;
        }

        public LoadBalanceStrategy strategy() {
            return strategy;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

    @KeepNames
    public static class ProcessorsLoadBalanceStrategyReceived {

        private final ProcessorsLBStrategy processorsStrategy;

        public ProcessorsLoadBalanceStrategyReceived(ProcessorsLBStrategy processorsStrategy) {
            this.processorsStrategy = processorsStrategy;
        }

        public ProcessorsLBStrategy processorsStrategy() {
            return processorsStrategy;
        }
    }

    @KeepNames
    public static class ProcessorLoadBalancingStrategyReceived {
        private final ProcessorLBStrategy processorLBStrategy;
        private final boolean proxied;

        public ProcessorLoadBalancingStrategyReceived(ProcessorLBStrategy processorLBStrategy, boolean proxied) {
            this.processorLBStrategy = processorLBStrategy;
            this.proxied = proxied;
        }

        public ProcessorLBStrategy processorLBStrategy() {
            return processorLBStrategy;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

}
