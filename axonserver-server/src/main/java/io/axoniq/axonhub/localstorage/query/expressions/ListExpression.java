package io.axoniq.axonhub.localstorage.query.expressions;

import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.ListExpressionResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class ListExpression implements Expression {
    private final String alias;
    private final Expression[] expressions;

    public ListExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.expressions = expressions;
    }

    public static ListExpression asListExpression(Expression expression) {
        if (expression instanceof ListExpression) {
            return (ListExpression) expression;
        } else {
            return new ListExpression(expression.alias(), new Expression[]{expression});
        }
    }

    @Override
    public ListExpressionResult apply(ExpressionContext context, ExpressionResult data) {
        List<ExpressionResult> results = new ArrayList<>(expressions.length);
        for (Expression item : expressions) {
            results.add(item.apply(context, data));
        }
        return new ListExpressionResult(results);
    }

    public List<Expression> items() {
        return Arrays.asList(expressions);
    }

    @Override
    public String alias() {
        return alias;
    }

    public List<String> columnNames() {
        return Arrays.stream(expressions).map(Expression::alias).collect(Collectors.toList());
    }
}
