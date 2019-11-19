package io.axoniq.axonserver.component.version;

import org.junit.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link BackwardsCompatibleVersion}
 *
 * @author Sara Pellegrini
 */
public class BackwardsCompatibleVersionTest {

    private final BackwardsCompatibleVersion version3 = new BackwardsCompatibleVersion("3");
    private final BackwardsCompatibleVersion version3_1 = new BackwardsCompatibleVersion("3.1");
    private final BackwardsCompatibleVersion version3_1_3 = new BackwardsCompatibleVersion("3.1.3");
    private final BackwardsCompatibleVersion version4 = new BackwardsCompatibleVersion("4");
    private final BackwardsCompatibleVersion version4_0 = new BackwardsCompatibleVersion("4.0");
    private final BackwardsCompatibleVersion version4_0_1 = new BackwardsCompatibleVersion("4.0.1");
    private final BackwardsCompatibleVersion version4_1 = new BackwardsCompatibleVersion("4.1");
    private final BackwardsCompatibleVersion version4_1_2 = new BackwardsCompatibleVersion("4.1.2");
    private final BackwardsCompatibleVersion version4_1_3 = new BackwardsCompatibleVersion("4.1.3");
    private final BackwardsCompatibleVersion version4_1_4 = new BackwardsCompatibleVersion("4.1.4");
    private final BackwardsCompatibleVersion version4_2 = new BackwardsCompatibleVersion("4.2");
    private final BackwardsCompatibleVersion version4_2_1 = new BackwardsCompatibleVersion("4.2.1");
    private final BackwardsCompatibleVersion version5 = new BackwardsCompatibleVersion("5");
    private final BackwardsCompatibleVersion version5_1 = new BackwardsCompatibleVersion("5.1");
    private final BackwardsCompatibleVersion version5_1_3 = new BackwardsCompatibleVersion("5.1.3");

    @Test
    public void testMatch() {
        boolean match = version4_1_3.match(new BackwardsCompatibleVersion("4.1.3"));
        assertTrue(match);

        match = version4_1_3.match(new BackwardsCompatibleVersion("4.1"));
        assertFalse(match);
    }

    @Test
    public void testGreaterOrEqualThan() {
        boolean greaterOrEqual = version4_1.greaterOrEqualThan(version4_1_3);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version4_1_2);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version4_1_4);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version4_2_1);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version4_0_1);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version5_1_3);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version3_1_3);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version4_1);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version4_2);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version4_0);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version5_1);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version3_1);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version4);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version3);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan(version5);
        assertFalse(greaterOrEqual);
    }

    @Test
    public void testGreaterOrEqualThanWithPatch() {
        boolean greaterOrEqual = version4_1_3.greaterOrEqualThan(version4_1_3);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version4_1_2);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version4_1_4);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version5_1_3);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version3_1_3);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version4_1);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version4_2);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version4_0);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version5_1);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version3_1);
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version4);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version3);
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan(version5);
        assertFalse(greaterOrEqual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooManyVersionNumbersInVersionClass() {
        version4_1_3.greaterOrEqualThan(new BackwardsCompatibleVersion("4.1.3.5"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooFewVersionNumbersInVersionClass() {
        version4_1_3.greaterOrEqualThan(new BackwardsCompatibleVersion(""));
    }
}