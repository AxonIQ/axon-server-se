/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;


import io.axoniq.EventStoreQueryLexer;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser to parse query string to {@link Query}. Throws {@link ParseException} when there is a syntax error in the query.
 * @author Marc Gathier
 * @since 4.0
 */
public class EventStoreQueryParser {

    public Query parse(String query) throws ParseException {
        EventStoreQueryLexer lexer = new EventStoreQueryLexer(new ANTLRInputStream(query));
        lexer.removeErrorListeners();
        List<ParseException> errors = new ArrayList<>();
        ANTLRErrorListener errorListener = new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                errors.add(new ParseException(msg, charPositionInLine));
            }
        };
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        io.axoniq.EventStoreQueryParser parser = new io.axoniq.EventStoreQueryParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        io.axoniq.EventStoreQueryParser.QueryContext query1 = parser.query();
        ParseTreeWalker walker = new ParseTreeWalker();

        EventStoreQueryListener baseListener = new EventStoreQueryListener(parser);
        walker.walk(baseListener, query1);
        if( ! errors.isEmpty()) {
            throw errors.get(0);
        }
        return baseListener.query();
    }

}
