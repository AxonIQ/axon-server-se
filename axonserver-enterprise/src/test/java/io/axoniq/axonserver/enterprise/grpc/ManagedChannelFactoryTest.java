package io.axoniq.axonserver.enterprise.grpc;

import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelFactory;
import io.grpc.ManagedChannel;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 24/04/2018.
 * sara.pellegrini@gmail.com
 */
public class ManagedChannelFactoryTest {

    @Test
    public void createSSLDisabled() {
        ManagedChannelFactory managedChannelFactory = new ManagedChannelFactory(false, null);
        ManagedChannel channel = managedChannelFactory.create("localhost", 1234);
        Assert.assertNotNull(channel);
    }
}