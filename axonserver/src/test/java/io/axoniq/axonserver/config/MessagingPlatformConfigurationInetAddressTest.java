package io.axoniq.axonserver.config;

import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;

import static io.axoniq.axonserver.config.MessagingPlatformConfiguration.DEFAULT_GRPC_PORT;
import static io.axoniq.axonserver.config.MessagingPlatformConfiguration.DEFAULT_INTERNAL_GRPC_PORT;
import static org.assertj.core.api.Assertions.assertThat;

class MessagingPlatformConfigurationInetAddressTest {

    private MessagingPlatformConfiguration testee = new MessagingPlatformConfiguration(new StubSystemInfoProvider());

    @Nested
    class PlatformTest {

        public static final int OTHER_PORT = 9124;
        public static final String OTHER_ADDRESS = "192.168.0.1";

        @Test
        void testReturnsDefaultSocket() {
            InetSocketAddress result = testee.getInetSocketAddress();

            assertThat(result.getPort()).isEqualTo(DEFAULT_GRPC_PORT);
            assertThat(result.getAddress().isAnyLocalAddress()).isTrue();
        }

        @Test
        void testRetainsDefaultAddressOnPortChange() {
            testee.setPort(OTHER_PORT);

            InetSocketAddress result = testee.getInetSocketAddress();

            assertThat(result.getPort()).isEqualTo(OTHER_PORT);
            assertThat(result.getAddress().isAnyLocalAddress()).isTrue();
        }

        @Test
        void testRetainsDefaultPortOnAddressChange() {
            testee.setIpAddress(OTHER_ADDRESS);

            InetSocketAddress result = testee.getInetSocketAddress();

            assertThat(result.getPort()).isEqualTo(DEFAULT_GRPC_PORT);
            assertThat(result.getAddress().getHostAddress()).isEqualTo(OTHER_ADDRESS);
        }

        @Test
        void testMergesAddressAndPortChanges() {
            testee.setPort(OTHER_PORT);
            testee.setIpAddress(OTHER_ADDRESS);

            InetSocketAddress result = testee.getInetSocketAddress();

            assertThat(result.getPort()).isEqualTo(OTHER_PORT);
            assertThat(result.getAddress().getHostAddress()).isEqualTo(OTHER_ADDRESS);
        }
    }

    @Nested
    class InternalTest {

        public static final int OTHER_PORT = 9224;
        public static final String OTHER_ADDRESS = "192.168.0.1";

        @Test
        void testReturnsDefaultSocket() {
            InetSocketAddress result = testee.getInternalInetSocketAddress();

            assertThat(result.getPort()).isEqualTo(DEFAULT_INTERNAL_GRPC_PORT);
            assertThat(result.getAddress().isAnyLocalAddress()).isTrue();
        }

        @Test
        void testRetainsDefaultAddressOnPortChange() {
            testee.setInternalPort(OTHER_PORT);

            InetSocketAddress result = testee.getInternalInetSocketAddress();

            assertThat(result.getPort()).isEqualTo(OTHER_PORT);
            assertThat(result.getAddress().isAnyLocalAddress()).isTrue();
        }

        @Test
        void testRetainsDefaultPortOnAddressChange() {
            testee.setInternalIpAddress(OTHER_ADDRESS);

            InetSocketAddress result = testee.getInternalInetSocketAddress();

            assertThat(result.getPort()).isEqualTo(DEFAULT_INTERNAL_GRPC_PORT);
            assertThat(result.getAddress().getHostAddress()).isEqualTo(OTHER_ADDRESS);
        }

        @Test
        void testMergesAddressAndPortChanges() {
            testee.setInternalPort(OTHER_PORT);
            testee.setInternalIpAddress(OTHER_ADDRESS);

            InetSocketAddress result = testee.getInternalInetSocketAddress();

            assertThat(result.getPort()).isEqualTo(OTHER_PORT);
            assertThat(result.getAddress().getHostAddress()).isEqualTo(OTHER_ADDRESS);
        }
    }


    private static class StubSystemInfoProvider implements SystemInfoProvider {

    }
}