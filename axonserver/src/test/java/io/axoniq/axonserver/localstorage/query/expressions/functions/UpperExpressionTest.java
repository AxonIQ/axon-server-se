package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.result.NullExpressionResult;
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.stringValue;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class UpperExpressionTest {
    private UpperExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new UpperExpression(null,
                new Identifier("value")
                );
        expressionContext = new ExpressionContext();
    }

    @Test
    public void upper() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abcdefg")));
        assertEquals("ABCDEFG", actual.getValue());
    }
    @Test
    public void upperNull() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value123", stringValue("UpperCase")));
        assertTrue(actual instanceof NullExpressionResult);
        assertNull( actual.getValue());
    }

}
