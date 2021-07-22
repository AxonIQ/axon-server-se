package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.message.command.InsufficientBufferCapacityException;
import junit.framework.TestCase;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class QueryCacheTest extends TestCase {

    private QueryCache testSubject;

    @Before
    public void setUp() {
        testSubject = new QueryCache(50000, 1);
    }

    @Test(expected = InsufficientBufferCapacityException.class)
    public void onFullCapacityThrowError() {

        testSubject.put("1234", mock(QueryInformation.class));
        testSubject.put("4567", mock(QueryInformation.class));
    }

    @Test
    public void cancelWithErrorOnTimeout() {
        QueryCache testSubject = new QueryCache(0, 1);
        QueryInformation queryInformation = mock(QueryInformation.class);
        QueryDefinition queryDefinition = mock(QueryDefinition.class);
        when(queryDefinition.getQueryName()).thenReturn("myQueryName");
        when(queryInformation.getQuery()).thenReturn(queryDefinition);
        when(queryInformation.getSourceClientId()).thenReturn("theRequester");
        when(queryInformation.waitingFor()).thenReturn(Collections.singleton("theResponder"));
        testSubject.put("myKey", queryInformation);
        testSubject.clearOnTimeout();
        verify(queryInformation).cancelWithError(eq(ErrorCode.QUERY_TIMEOUT),
                                                 matches("Query cancelled due to timeout"));
    }
}