package io.axoniq.axonhub.localstorage.query.expressions.functions;

import com.jayway.jsonpath.JsonPath;
import io.axoniq.axonhub.localstorage.query.Expression;
import io.axoniq.axonhub.localstorage.query.ExpressionContext;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.JSONExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.ListExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.NumericExpressionResult;
import io.axoniq.axonhub.localstorage.query.result.StringExpressionResult;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: marc
 */
public class JsonPathExpression implements Expression {
    private final String alias;
    private final Expression document;
    private final Expression jsonPath;

    public JsonPathExpression(String alias, Expression[] expressions) {
        this.alias = alias;
        this.document = expressions[0];
        this.jsonPath = expressions[1];
    }

    @Override
    public ExpressionResult apply(ExpressionContext expressionContext, ExpressionResult input) {
        ExpressionResult doc = document.apply(expressionContext, input);
        String json = jsonPath.apply(expressionContext, input).getValue().toString();
        Object result = JsonPath.compile(json).read(doc.asJson());
        List<ExpressionResult> values = new ArrayList<>();
        boolean isList = false;
        if( result instanceof JSONArray) {
            isList = true;
            JSONArray resultArray = (JSONArray)result;
            resultArray.forEach(value ->  values.add(toExpressionResult(value)));
        } else {
            values.add(toExpressionResult(result));
        }
        if( ! isList) {
            if( values.size() == 1 ) return values.get(0);
            return NullExpressionResult.INSTANCE;
        }
        return new ListExpressionResult(values);
    }

    private ExpressionResult toExpressionResult(Object value) {
        if( value instanceof String) return new StringExpressionResult((String)value);
        if( value instanceof Number) return new NumericExpressionResult(((Number)value).doubleValue());
        if( value instanceof JSONObject) return new JSONExpressionResult(((JSONObject)value));
        return NullExpressionResult.INSTANCE;
    }

    @Override
    public String alias() {
        return alias;
    }
}
