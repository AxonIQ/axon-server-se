package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.expressions.NumericLiteral;
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.mapValue;
import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.stringValue;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class RightExpressionTest {
    private RightExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new RightExpression(null, new Expression[] {
                new Identifier("value"),
                new NumericLiteral(null, "2")
                });
        expressionContext = new ExpressionContext();
    }

    @Test
    public void normalRight() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abcdefg")));
        assertEquals("fg", actual.getValue());
    }

    @Test
    public void tooShort() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("a")));
        assertEquals("a", actual.getValue());
    }

}