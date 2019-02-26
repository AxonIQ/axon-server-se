package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
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
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class AuthenticationInterceptorTest {
    private AuthenticationInterceptor testSubject;

    @Mock
    private AxonServerAccessController accessController;

    @Mock
    private ServerCallHandler<String, String> handler;

    @Mock
    private ServerCall<String, String> call;

    @Mock
    private MethodDescriptor.Marshaller<String> marshaller;

    private MethodDescriptor<String, String> path1;
    private MethodDescriptor<String, String> path2;

    private ArgumentCaptor<Status> status;
    private ArgumentCaptor<Metadata> trailers;
    private Metadata metadata = new Metadata();


    @Before
    public void setup() {
        when(accessController.allowed(eq("path1"), any(), eq("1234"))).thenReturn(false);
        when(accessController.allowed(eq("path2"), any(), eq("1234"))).thenReturn(true);
        testSubject = new AuthenticationInterceptor(accessController);

        status = ArgumentCaptor.forClass(Status.class);
        trailers = ArgumentCaptor.forClass(Metadata.class);
        path1 =
                MethodDescriptor.<String, String>newBuilder().setFullMethodName("path1")
                        .setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
                        .setRequestMarshaller(marshaller)
                        .setResponseMarshaller(marshaller)
                        .build();
        path2 =
                MethodDescriptor.<String, String>newBuilder().setFullMethodName("path2")
                        .setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
                        .setRequestMarshaller(marshaller)
                        .setResponseMarshaller(marshaller)
                        .build();
    }

    @Test
    public void noToken() throws Exception {
        testSubject.interceptCall(call, metadata, handler);
        verify(call).close(status.capture(), trailers.capture());
        assertEquals(PERMISSION_DENIED, status.getValue().getCode());
        assertEquals("AXONIQ-1000", trailers.getValue().get(GrpcMetadataKeys.ERROR_CODE_KEY));
        verify(handler, times(0)).startCall(any(), any());
    }

    @Test
    public void invalidToken() throws Exception {
        metadata.put(GrpcMetadataKeys.TOKEN_KEY, "1234");
        when(call.getMethodDescriptor()).thenReturn(path1);

        testSubject.interceptCall(call, metadata, handler);

        verify(call).close(status.capture(), trailers.capture());
        assertEquals(PERMISSION_DENIED, status.getValue().getCode());
        assertEquals("AXONIQ-1001", trailers.getValue().get(GrpcMetadataKeys.ERROR_CODE_KEY));
        verify(handler, times(0)).startCall(any(), any());
    }

    @Test
    public void validToken() throws Exception {
        metadata.put(GrpcMetadataKeys.TOKEN_KEY, "1234");
        when(call.getMethodDescriptor()).thenReturn(path2);

        testSubject.interceptCall(call, metadata, handler);

        verify(handler, times(1)).startCall(any(), any());
    }

}
