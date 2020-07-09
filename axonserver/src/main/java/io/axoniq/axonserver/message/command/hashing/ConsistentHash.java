/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command.hashing;

import org.springframework.util.Assert;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class ConsistentHash {

    private final SortedMap<String, ConsistentHashMember> hashToMember;

    /**
     * Initializes a new {@link ConsistentHash}. To register members use {@link #with(String, int)}.
     */
    public ConsistentHash() {
        hashToMember = Collections.emptySortedMap();
    }

    private ConsistentHash(SortedMap<String, ConsistentHashMember> hashed) {
        hashToMember = hashed;
    }

    /**
     * Returns the hash of the given {@code routingKey}. By default this creates a MD5 hash with hex encoding.
     *
     * @param routingKey the routing key to hash
     * @return a hash of the input key
     */
    protected static String hash(String routingKey) {
        return Digester.md5Hex(routingKey);
    }

    /**
     * Returns the member instance to which the given {@code message} should be routed. If no suitable member could be
     * found an empty Optional is returned.
     *
     * @param routingKey the routing that should be used to select a member
     * @param candidates set of allowed candidates, null means all candidates allowed
     * @return the member that should handle the message or an empty Optional if no suitable member was found
     */
    public Optional<ConsistentHashMember> getMember(String routingKey,
                                                    Set<String> candidates) {

        String hash = hash(routingKey);
        SortedMap<String, ConsistentHashMember> tailMap = hashToMember.tailMap(hash);
        Iterator<Map.Entry<String, ConsistentHashMember>> tailIterator = tailMap.entrySet().iterator();
        Optional<ConsistentHashMember> foundMember = findSuitableMember(tailIterator, candidates);
        if (!foundMember.isPresent()) {
            Iterator<Map.Entry<String, ConsistentHashMember>> headIterator =
                    hashToMember.headMap(hash).entrySet().iterator();
            foundMember = findSuitableMember(headIterator, candidates);
        }
        return foundMember;
    }

    private Optional<ConsistentHashMember> findSuitableMember(
            Iterator<Map.Entry<String, ConsistentHashMember>> iterator,
            Set<String> bestMatches) {
        while (iterator.hasNext()) {
            Map.Entry<String, ConsistentHashMember> entry = iterator.next();
            if (bestMatches == null || bestMatches.contains(entry.getValue().member)) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the set of members registered with this consistent hash instance.
     *
     * @return the members of this consistent hash
     */
    public Set<ConsistentHashMember> getMembers() {
        return Collections.unmodifiableSet(new HashSet<>(hashToMember.values()));
    }

    /**
     * Registers the given {@code member} with given {@code loadFactor} and {@code commandFilter} if it is not
     * already contained in the {@link ConsistentHash}. It will return the current ConsistentHash if the addition is
     * a duplicate and returns a new ConsistentHash with updated memberships if it is not.
     * <p>
     * The relative loadFactor of the member determines the likelihood of being selected as a destination for a command.
     *
     * @param member     the member to register
     * @param loadFactor the load factor of the new member
     * @return a new {@link ConsistentHash} instance with updated memberships
     */
    public ConsistentHash with(String member, int loadFactor) {
        Assert.notNull(member, "Member may not be null");

        ConsistentHashMember newMember = new ConsistentHashMember(member, loadFactor);
        if (getMembers().contains(newMember)) {
            return this;
        }

        SortedMap<String, ConsistentHashMember> members = new TreeMap<>(without(member).hashToMember);
        newMember.hashes().forEach(h -> members.put(h, newMember));

        return new ConsistentHash(members);
    }

    /**
     * Deregisters the given {@code member} and returns a new {@link ConsistentHash} with updated memberships.
     *
     * @param member the member to remove from the consistent hash
     * @return a new {@link ConsistentHash} instance with updated memberships
     */
    public ConsistentHash without(String member) {
        Assert.notNull(member, "Member may not be null");
        SortedMap<String, ConsistentHashMember> newHashes = new TreeMap<>();
        this.hashToMember.forEach((h, v) -> {
            if (!Objects.equals(v.getClient(), member)) {
                newHashes.put(h, v);
            }
        });
        return new ConsistentHash(newHashes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsistentHash that = (ConsistentHash) o;
        return Objects.equals(hashToMember, that.hashToMember);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashToMember);
    }

    public boolean contains(String client) {
        return getMembers().stream().anyMatch(member -> member.member.equals(client));
    }

    @Override
    public String toString() {
        return "ConsistentHash [" +
                hashToMember.values().stream().map(ConsistentHashMember::toString).collect(Collectors.joining(",")) +
                "]";
    }

    /**
     * Member implementation used by a {@link ConsistentHash} registry.
     */
    public static class ConsistentHashMember {

        private final String member;
        private final int segmentCount;

        private ConsistentHashMember(String member, int segmentCount) {
            this.member = member;
            this.segmentCount = segmentCount;
        }

        public String getClient() {
            return member;
        }

        /**
         * Returns this member's segment count which relates to the relative load factor of the member.
         *
         * @return the member's segment count
         */
        public int segmentCount() {
            return segmentCount;
        }

        /**
         * Returns the hashes covered by the member. If the hash of the routing key matches with one of the returned
         * hashes and the member is capable of handling the command then it will be selected as a target for the
         * command.
         *
         * @return the hashes covered by this member
         */
        public Set<String> hashes() {
            Set<String> newHashes = new TreeSet<>();
            for (int t = 0; t < segmentCount; t++) {
                String hash = hash(getClient() + " #" + t);
                newHashes.add(hash);
            }
            return newHashes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConsistentHashMember that = (ConsistentHashMember) o;
            return segmentCount == that.segmentCount &&
                    Objects.equals(member, that.member);
        }

        @Override
        public int hashCode() {
            return Objects.hash(member, segmentCount);
        }

        @Override
        public String toString() {
            return member + "(" + segmentCount + ")";
        }
    }
}
