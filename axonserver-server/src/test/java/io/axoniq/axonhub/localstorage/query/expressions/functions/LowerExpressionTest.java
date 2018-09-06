package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.expressions.Identifier;
import org.junit.*;

import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.stringValue;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class LowerExpressionTest {
    private LowerExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new LowerExpression(null,
                new Identifier("value")
                );
        expressionContext = new ExpressionContext();
    }

    @Test
    public void lower() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("UpperCase")));
        assertEquals("uppercase", actual.getValue());
    }
    @Test
    public void lowerNull() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value123", stringValue("UpperCase")));
        assertNull( actual.getValue());
    }
}