package io.axoniq.axonhub.localstorage.query.expressions.binary;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.expressions.Identifier;
import io.axoniq.axonhub.localstorage.query.expressions.ListExpression;
import io.axoniq.axonhub.localstorage.query.expressions.NumericLiteral;
import io.axoniq.axonhub.localstorage.query.expressions.StringLiteral;
import org.junit.*;

import static io.axoniq.axonhub.localstorage.query.expressions.ResultFactory.*;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class InExpressionTest {
    private InExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new InExpression(null, new Expression[]{
                new Identifier("value"),
                new ListExpression(null, new Expression[]{
                        new StringLiteral("a"),
                        new Identifier("otherValue"),
                        new StringLiteral("b"),
                        new NumericLiteral(null, "100")
                })
        });
        expressionContext = new ExpressionContext();
    }

    @Test
    public void inContainsString() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("b")));
        assertTrue(actual.isTrue());
    }

    @Test
    public void inContainsNumber() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", numericValue(100)));
        assertTrue(actual.isTrue());
    }

    @Test
    public void inNotContainsString() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("q")));
        assertFalse(actual.isTrue());
    }

    @Test
    public void inNotContainsNumber() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", numericValue(1000)));
        assertFalse(actual.isTrue());
    }

    @Test
    public void inContainsIdentifier() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("c"),
                "otherValue", stringValue("c")));
        assertTrue(actual.isTrue());
    }

    @Test
    public void inNotContainsIdentifier() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("c"),
                "otherValue", stringValue("qqqq")));
        assertFalse(actual.isTrue());
    }
}