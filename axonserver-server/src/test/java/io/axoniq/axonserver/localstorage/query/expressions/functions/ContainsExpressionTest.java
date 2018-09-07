package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.expressions.Identifier;
import io.axoniq.axonserver.localstorage.query.expressions.NumericLiteral;
import io.axoniq.axonserver.localstorage.query.expressions.StringLiteral;
import org.junit.*;

import static io.axoniq.axonserver.localstorage.query.expressions.ResultFactory.*;
import static org.junit.Assert.*;

public class ContainsExpressionTest {

    private ContainsExpression testSubject;
    private ExpressionContext expressionContext;

    @Before
    public void setUp() {
        expressionContext = new ExpressionContext();
        testSubject = new ContainsExpression("contains", new Expression[]{new Identifier("value"), new StringLiteral("xyz")});
    }

    @Test
    public void testStringContainsGivenValue() {
        ExpressionResult actualResult = testSubject.apply(expressionContext, mapValue("value", stringValue("someValue_xyz_Containing")));
        assertTrue(actualResult.isTrue());
    }

    @Test
    public void testStringContainsDifferentCase() {
        ExpressionResult actualResult = testSubject.apply(expressionContext, mapValue("value", stringValue("someValue_XYZ_Containing")));
        assertFalse(actualResult.isTrue());
    }

    @Test
    public void testValueIsNumeric() {
        ExpressionResult actualResult = testSubject.apply(expressionContext, mapValue("value", numericValue(1L)));
        assertFalse(actualResult.isTrue());
    }

    @Test
    public void testContainsMatchesElementInList() {
        ExpressionResult actualResult = testSubject.apply(expressionContext, mapValue("value", listValue(stringValue("xyz"), numericValue(1L))));
        assertTrue(actualResult.isTrue());
    }

    @Test
    public void testContainsDoesntMatchElementInList() {
        ExpressionResult actualResult = testSubject.apply(expressionContext, mapValue("value", listValue(stringValue("someValue_xyz_Containing"), numericValue(1L))));
        assertFalse(actualResult.isTrue());
    }

    @Test
    public void testContainsMatchesNumericValueInCollection() {
        testSubject = new ContainsExpression("contains", new Expression[]{new Identifier("value"), new NumericLiteral("nr", "1")});

        ExpressionResult actualResult = testSubject.apply(expressionContext, mapValue("value", listValue(stringValue("someValue_xyz_Containing"), numericValue(1L))));
        assertTrue(actualResult.isTrue());
    }
}
