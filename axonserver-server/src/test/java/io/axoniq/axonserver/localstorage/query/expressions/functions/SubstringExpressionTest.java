package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.expressions.NumericLiteral;
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.*;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class SubstringExpressionTest {
    private SubstringExpression testSubject;
    private SubstringExpression testSubject2;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        testSubject = new SubstringExpression(null, new Expression[] {
                new Identifier("value"),
                new NumericLiteral(null, "1"),
                new NumericLiteral(null, "4" )});
        testSubject2 = new SubstringExpression(null, new Expression[] {
                new Identifier("value"),
                new Identifier("start")});
        expressionContext = new ExpressionContext();
    }

    @Test
    public void normalSubstring() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abcdefg")));
        assertEquals("bcd", actual.getValue());
    }

    @Test
    public void normalSubstringOnlyStart() {
        ExpressionResult actual = testSubject2.apply(expressionContext, mapValue("value", stringValue("abcdefg"), "start", numericValue(1)));
        assertEquals("bcdefg", actual.getValue());
    }

    @Test
    public void tooShort() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("abc")));
        assertEquals("bc", actual.getValue());
    }

    @Test
    public void tooShort2() {
        ExpressionResult actual = testSubject.apply(expressionContext, mapValue("value", stringValue("")));
        assertEquals("", actual.getValue());
    }

}