/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;

import io.axoniq.EventStoreQueryBaseListener;
import io.axoniq.EventStoreQueryParser;

import java.util.Stack;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public class EventStoreQueryListener extends EventStoreQueryBaseListener {
    Stack<PipelineEntry> stack = new Stack<>();
    private final io.axoniq.EventStoreQueryParser queryParser;

    public EventStoreQueryListener(EventStoreQueryParser queryParser) {
        this.queryParser = queryParser;
    }


    @Override
    public void enterQuery(EventStoreQueryParser.QueryContext ctx) {
        stack.push(new Query());
    }

    @Override
    public void exitQuery(EventStoreQueryParser.QueryContext ctx) {
    }

    @Override
    public void enterTime_constraint(EventStoreQueryParser.Time_constraintContext ctx) {
        TimeConstraint timeConstraint = new TimeConstraint();
        timeConstraint.add(new Identifier(normalizeSymbolicName(ctx.start.getType())));
        stack.push(timeConstraint);
    }

    private String normalizeSymbolicName(int type) {
        String symbolicName = queryParser.getVocabulary().getSymbolicName(type);
        if( symbolicName != null){
            switch (symbolicName) {
                case "K_LAST":
                    return "last";
                case "K_SECOND":
                    return "second";
                case "K_MINUTE":
                    return "minute";
                case "K_HOUR":
                    return "hour";
                case "K_DAY":
                    return "day";
                case "K_WEEK":
                    return "week";
                case "K_MONTH":
                    return "month";
                case "K_YEAR":
                    return "year";
            }
        }
        return null;

    }

    @Override
    public void exitTime_constraint(EventStoreQueryParser.Time_constraintContext ctx) {
        PipelineEntry pipelineEntry = stack.pop();
        stack.peek().add(pipelineEntry);
    }

    @Override
    public void exitTime_unit(EventStoreQueryParser.Time_unitContext ctx) {
        stack.peek().add(new Identifier(normalizeSymbolicName(ctx.stop.getType())));
    }


    @Override
    public void enterExpr(EventStoreQueryParser.ExprContext ctx) {
        FunctionExpr function = new FunctionExpr();
        stack.push(function);
    }

    @Override
    public void exitExpr(EventStoreQueryParser.ExprContext ctx) {
        FunctionExpr expr = (FunctionExpr)stack.pop();
        if( expr.children.size() == 1 &&  (
                expr.children.get(0) instanceof Numeric
                || expr.children.get(0) instanceof StringLiteral
                || expr.children.get(0) instanceof OperandList || expr.children.get(0) instanceof Identifier
                || expr.children.get(0) instanceof FunctionExpr
        ) ) {
            stack.peek().add(expr.children.get(0));
        } else {
            stack.peek().add(expr);
        }
    }

    @Override
    public void exitSigned_number(EventStoreQueryParser.Signed_numberContext ctx) {
        stack.peek().add(new Numeric(ctx.getText()));
    }

    @Override
    public void exitLiteral_value(EventStoreQueryParser.Literal_valueContext ctx) {
        stack.peek().add(new StringLiteral(ctx.getText()));
    }

    @Override
    public void exitIdentifier(EventStoreQueryParser.IdentifierContext ctx) {
        stack.peek().add(new Identifier(ctx.getText()));
    }

    @Override
    public void exitFunction_name(EventStoreQueryParser.Function_nameContext ctx) {
        FunctionExpr expr = (FunctionExpr)stack.peek();
        expr.setOperator(ctx.getText());
    }

    @Override
    public void exitUnary_operator(EventStoreQueryParser.Unary_operatorContext ctx) {
        FunctionExpr expr = (FunctionExpr)stack.peek();
        expr.setOperator(ctx.getText().toLowerCase());
    }

    @Override
    public void exitOperator_group1(EventStoreQueryParser.Operator_group1Context ctx) {
        FunctionExpr expr = (FunctionExpr)stack.peek();
        expr.setOperator(ctx.getText().toLowerCase());
    }

    @Override
    public void exitOperator_group2(EventStoreQueryParser.Operator_group2Context ctx) {
        FunctionExpr expr = (FunctionExpr)stack.peek();
        expr.setOperator(ctx.getText().toLowerCase());
    }

    @Override
    public void exitOperator_group3(EventStoreQueryParser.Operator_group3Context ctx) {
        FunctionExpr expr = (FunctionExpr)stack.peek();
        expr.setOperator(ctx.getText().toLowerCase());
    }

    @Override
    public void exitOperator_group4(EventStoreQueryParser.Operator_group4Context ctx) {
        FunctionExpr expr = (FunctionExpr)stack.peek();
        expr.setOperator(ctx.getText().toLowerCase());
    }

    @Override
    public void exitOperator_group5(EventStoreQueryParser.Operator_group5Context ctx) {
        FunctionExpr expr = (FunctionExpr)stack.peek();
        expr.setOperator(ctx.getText().toLowerCase());
    }

    @Override
    public void exitOperator_group6(EventStoreQueryParser.Operator_group6Context ctx) {
        FunctionExpr expr = (FunctionExpr)stack.peek();
        expr.setOperator(ctx.getText().toLowerCase());
    }

    @Override
    public void exitAlias(EventStoreQueryParser.AliasContext ctx) {
        BaseQueryElement expr = (BaseQueryElement)stack.peek();
        PipelineEntry last = expr.children.get(expr.children.size() - 1);
        if( last instanceof FunctionExpr) ((FunctionExpr) last).setAlias(ctx.getText());
        if( last instanceof Identifier) ((Identifier) last).setAlias(ctx.getText());
    }

    @Override
    public void enterExpr_list(EventStoreQueryParser.Expr_listContext ctx) {
        stack.push(new OperandList());
    }

    @Override
    public void exitExpr_list(EventStoreQueryParser.Expr_listContext ctx) {
        PipelineEntry pipelineEntry = stack.pop();
        stack.peek().add(pipelineEntry);
    }

    public Query query() {
        return (Query) stack.pop();
    }

}
