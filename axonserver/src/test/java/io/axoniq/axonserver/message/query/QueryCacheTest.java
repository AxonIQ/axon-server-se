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

        testSubject.put("1234", mock(ActiveQuery.class));
        testSubject.put("4567", mock(ActiveQuery.class));
    }

    @Test
    public void cancelWithErrorOnTimeout() {
        QueryCache testSubject = new QueryCache(0, 1);
        ActiveQuery activeQuery = mock(ActiveQuery.class);
        QueryDefinition queryDefinition = mock(QueryDefinition.class);
        when(queryDefinition.getQueryName()).thenReturn("myQueryName");
        when(activeQuery.getQuery()).thenReturn(queryDefinition);
        when(activeQuery.getSourceClientId()).thenReturn("theRequester");
        when(activeQuery.waitingFor()).thenReturn(Collections.singleton("theResponder"));
        testSubject.put("myKey", activeQuery);
        testSubject.clearOnTimeout();
        verify(activeQuery).cancelWithError(eq(ErrorCode.QUERY_TIMEOUT),
                                            matches("Query cancelled due to timeout"));
    }
}