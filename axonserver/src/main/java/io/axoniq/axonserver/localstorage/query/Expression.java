package io.axoniq.axonserver.localstorage.query;

/**
 * @author Marc Gathier
 */
public interface Expression {

    ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input);

    String alias();
}
