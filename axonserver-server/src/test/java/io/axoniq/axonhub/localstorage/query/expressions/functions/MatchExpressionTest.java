package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.expressions.Identifier;
import org.junit.*;

import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.stringValue;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class MatchExpressionTest {
    private MatchExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new MatchExpression(null, new Expression[] {
                new Identifier("value"),
                new Identifier( "pattern")
                });
        expressionContext = new ExpressionContext();
    }

    @Test
    public void normalMatch() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abcdefg"),
                "pattern", stringValue("a.*g")));
        assertTrue( actual.isTrue());
    }

    @Test
    public void nonMatch() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abcdefg"),
                "pattern", stringValue("b.*g")));
        assertFalse( actual.isTrue());
    }

    @Test
    public void nullMatch() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue(
                "pattern", stringValue("b.*g")));
        assertFalse( actual.isTrue());
    }
}