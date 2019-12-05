/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command.hashing;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Marc Gathier
 */
public class Digester {

    public static final String MD_5 = "MD5";
    public static final String UTF_8 = "UTF-8";
    private final MessageDigest messageDigest;

    private Digester(MessageDigest messageDigest) {
        this.messageDigest = messageDigest;
    }

    /**
     * Creates a new Digester instance for the given {@code algorithm}.
     *
     * @param algorithm The algorithm to use, e.g. "MD5"
     * @return a fully initialized Digester instance
     */
    public static Digester newInstance(String algorithm) {
        try {
            return new Digester(MessageDigest.getInstance(algorithm));
        } catch (NoSuchAlgorithmException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "This environment doesn't support the MD5 hashing algorithm",
                                                 e);
        }
    }

    /**
     * Creates a new Digester instance for the MD5 Algorithm
     *
     * @return a Digester instance to create MD5 hashes
     */
    public static Digester newMD5Instance() {
        return newInstance(MD_5);
    }

    /**
     * Utility method that creates a hex string of the MD5 hash of the given {@code input}
     *
     * @param input The value to create a MD5 hash for
     * @return The hex representation of the MD5 hash of given {@code input}
     */
    public static String md5Hex(String input) {
        try {
            return newMD5Instance().update(input.getBytes(UTF_8)).digestHex();
        } catch (UnsupportedEncodingException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "The UTF-8 encoding is not available on this environment",
                                                 e);
        }
    }

    private static String hex(byte[] hash) {
        return pad(new BigInteger(1, hash).toString(16));
    }

    private static String pad(String md5) {
        if (md5.length() == 32) {
            return md5;
        }
        StringBuilder sb = new StringBuilder(32);
        for (int t = 0; t < 32 - md5.length(); t++) {
            sb.append("0");
        }
        sb.append(md5);
        return sb.toString();
    }

    /**
     * Update the Digester with given {@code additionalData}.
     *
     * @param additionalData The data to add to the digest source
     * @return {@code this} for method chaining
     */
    public Digester update(byte[] additionalData) {
        messageDigest.update(additionalData);
        return this;
    }

    /**
     * Returns the hex representation of the digest of all data that has been provided so far.
     *
     * @return the hex representation of the digest of all data that has been provided so far
     *
     * @see #update(byte[])
     */
    public String digestHex() {
        return hex(messageDigest.digest());
    }
}
