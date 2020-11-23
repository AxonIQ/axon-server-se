package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.message.command.InsufficientBufferCapacityException;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;

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