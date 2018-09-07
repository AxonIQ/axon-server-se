package io.axoniq.axonserver.localstorage.query;

/**
 * Author: marc
 */
public interface Expression {

    ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input);

    String alias();
}
