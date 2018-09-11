package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.GrpcMetadataKeys;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import static io.grpc.Status.Code.PERMISSION_DENIED;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class InternalAuthenticationInterceptorTest {
    private InternalAuthenticationInterceptor testSubject;
    private Metadata metadata = new Metadata();

    @Mock
    private ServerCall<String, String> call;
    @Mock
    private ServerCallHandler<String, String> handler;

    private ArgumentCaptor<Status> status;
    private ArgumentCaptor<Metadata> trailers;


    @Before
    public void setUp() throws Exception {
        MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        configuration.setAccesscontrol(new AccessControlConfiguration());
        configuration.getAccesscontrol().setInternalToken("MyInternalToken");

        status = ArgumentCaptor.forClass(Status.class);
        trailers = ArgumentCaptor.forClass(Metadata.class);

        testSubject = new InternalAuthenticationInterceptor(configuration);
    }

    @Test
    public void interceptCallWithoutToken() throws Exception {
        testSubject.interceptCall(call, metadata, handler);
        verify(call).close(status.capture(), trailers.capture());
        assertEquals(PERMISSION_DENIED, status.getValue().getCode());
        assertEquals("AXONIQ-1000", trailers.getValue().get(GrpcMetadataKeys.ERROR_CODE_KEY));
        verify(handler, times(0)).startCall(any(), any());
    }

    @Test
    public void interceptCallWithInvalidToken() throws Exception {
        metadata.put( GrpcMetadataKeys.INTERNAL_TOKEN_KEY, "TestToken");
        testSubject.interceptCall(call, metadata, handler);
        verify(call).close(status.capture(), trailers.capture());
        assertEquals(PERMISSION_DENIED, status.getValue().getCode());
        assertEquals("AXONIQ-1001", trailers.getValue().get(GrpcMetadataKeys.ERROR_CODE_KEY));
        verify(handler, times(0)).startCall(any(), any());
    }

    @Test
    public void interceptCallWithValidToken() throws Exception {
        metadata.put( GrpcMetadataKeys.INTERNAL_TOKEN_KEY, "MyInternalToken");
        testSubject.interceptCall(call, metadata, handler);
        verify(call, times(0)).close(status.capture(), trailers.capture());
        verify(handler).startCall(any(), any());
    }
}