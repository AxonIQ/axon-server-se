package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.expressions.Identifier;
import io.axoniq.axonhub.localstorage.query.result.NullExpressionResult;
import org.junit.*;

import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.stringValue;
import static org.junit.Assert.*;

/**
 * Author: marc
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