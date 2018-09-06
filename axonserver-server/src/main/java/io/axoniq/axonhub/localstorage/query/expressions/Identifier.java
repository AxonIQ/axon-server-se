package io.axoniq.axonhub.localstorage.query.expressions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;

/**
 * Author: marc
 */
public class Identifier implements Expression {

    private final String[] identifiers;
    private final String alias;

    public Identifier(String identifierName) {
        this( identifierName, identifierName);
    }

    public Identifier(String alias, String identifierName) {
        this.identifiers = identifierName.split("\\.");
        this.alias = alias;
    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult data) {
        if( data == null) return null;
        ExpressionResult result = data;
        for (String identifier : identifiers) {
            result = result.getByIdentifier(identifier);
        }
        return result;
    }

    @Override
    public String alias() {
        return alias;
    }
}
