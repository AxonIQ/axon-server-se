package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.refactoring.messaging.InsufficientBufferCapacityException;
import io.axoniq.axonserver.refactoring.messaging.query.QueryCache;
import io.axoniq.axonserver.refactoring.messaging.query.QueryInformation;
import junit.framework.TestCase;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

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

}