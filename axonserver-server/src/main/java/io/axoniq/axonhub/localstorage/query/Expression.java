package io.axoniq.axonhub.localstorage.query;

/**
 * Author: marc
 */
public interface Expression {

    ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input);

    String alias();
}
