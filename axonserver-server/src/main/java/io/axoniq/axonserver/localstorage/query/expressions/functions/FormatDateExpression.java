package io.axoniq.axonserver.localstorage.query.expressions.functions;

import io.axoniq.axonserver.localstorage.query.Expression;
import io.axoniq.axonserver.localstorage.query.ExpressionContext;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import io.axoniq.axonserver.localstorage.query.PipeExpression;
import io.axoniq.axonserver.localstorage.query.Pipeline;
import io.axoniq.axonserver.localstorage.query.QueryResult;
import io.axoniq.axonserver.localstorage.query.expressions.StringLiteral;
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NullExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.StringExpressionResult;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static java.util.Collections.singletonMap;

/**
 * Author: marc
 */
public class FormatDateExpression implements Expression, PipeExpression {

    private final String alias;
    private final Expression valueExpression;
    private final Expression formatExpression;
    private final Expression timezoneExpression;

    public FormatDateExpression(String alias, Expression... parameters) {
        this.alias = alias;
        this.valueExpression = parameters[0];
        this.formatExpression = parameters.length > 1 ? parameters[1] : new StringLiteral("yyyy-MM-dd HH:mm:ss");
        this.timezoneExpression = parameters.length > 2 ? parameters[2] : null;

    }

    @Override
    public ExpressionResult apply(ExpressionContext context, ExpressionResult input) {
        ExpressionResult value = valueExpression.apply(context, input);
        return value.isNonNull() ?  new StringExpressionResult(format(context, input, value.getNumericValue())) : NullExpressionResult.INSTANCE ;
    }

    private String format(ExpressionContext context, ExpressionResult input, BigDecimal value) {
        String format = formatExpression.apply(context, input).toString();
        DateFormat dateFormat = new SimpleDateFormat(format);
        TimeZone timeZone = TimeZone.getDefault();
        if( timezoneExpression != null) {
            timeZone = TimeZone.getTimeZone(timezoneExpression.apply(context, input).toString());
        }
        dateFormat.setTimeZone(timeZone);
        return dateFormat.format(Date.from(Instant.ofEpochMilli(value.longValue())));
    }

    @Override
    public String alias() {
        return alias;
    }

    @Override
    public boolean process(ExpressionContext context, QueryResult result, Pipeline next) {
        ExpressionResult apply = apply(context, result.getValue());
        return next.process(result.withValue(new MapExpressionResult(singletonMap(alias(), apply))));
    }

    @Override
    public List<String> getColumnNames(List<String> inputColumns) {
        return Collections.singletonList(alias);
    }


}
