package io.axoniq.axonhub.localstorage.query.expressions.functions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.expressions.Identifier;
import io.axoniq.axonhub.localstorage.query.expressions.StringLiteral;
import org.junit.*;

import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.*;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class ConcatExpressionTest {
    private ConcatExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new ConcatExpression(null, new Expression[] {
                new Identifier("value"),
                new StringLiteral( "---")
                });
        expressionContext = new ExpressionContext();
    }

    @Test
    public void concat() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", listValue(stringValue("a"), stringValue("b"), numericValue(12))));
        assertEquals("a---b---12", actual.getValue());
    }

}