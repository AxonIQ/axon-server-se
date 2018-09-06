package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.expressions.Identifier;
import io.axoniq.axonhub.localstorage.query.result.NumericExpressionResult;
import org.junit.*;

import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.*;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class AddExpressionTest {
    private AddExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setup() {
        Expression[] expressions = {
                new Identifier("first"),
                new Identifier("second")
        };
        testSubject = new AddExpression("add", expressions);
        expressionContext = new ExpressionContext();
    }

    @Test
    public void addNumbers() {
        ExpressionResult result =
                testSubject.apply(expressionContext, mapValue("first", numericValue(100), "second", numericValue(200)));
        assertTrue( result instanceof NumericExpressionResult);
        assertEquals(300, result.getNumericValue().longValue());
    }

    @Test
    public void addString() {
        ExpressionResult result = testSubject.apply(expressionContext, mapValue("first", stringValue("100"), "second", numericValue(200)));
        assertEquals("100200", result.toString());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void applyWithFirstNull() {
        testSubject.apply(expressionContext, mapValue("first", nullValue(), "second", numericValue(200)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void applyWithInvalidSecond() {
        testSubject.apply(expressionContext, mapValue("first", numericValue(100), "second", stringValue("200")));
    }
}
