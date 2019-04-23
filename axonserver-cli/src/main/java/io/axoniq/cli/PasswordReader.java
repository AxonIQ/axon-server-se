/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import java.util.Scanner;
import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public class PasswordReader {
    public static final PasswordReader INSTANCE = new PasswordReader();

    private final Function<String,String> console = System.console() == null ? this::readFromStdin : text -> String.valueOf(System.console().readPassword(text));

    private PasswordReader() {
    }

    public String readPassword() {
        while( true) {
            String password1 = console.apply("Enter password for user: ");
            String password2 = console.apply("Re-enter password for user: ");
            if (password1.equals(password2)) return password1;

            System.out.println( "Passwords are not the same, please enter again.");
        }
    }

    private String readFromStdin(String text) {
        Scanner scanner = new Scanner(System.in);
        System.out.print( text);
        return scanner.nextLine();
    }


}
