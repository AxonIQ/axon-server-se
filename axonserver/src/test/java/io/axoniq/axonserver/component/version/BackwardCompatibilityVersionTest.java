package io.axoniq.axonserver.component.version;

import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class BackwardCompatibilityVersionTest {

    BackwardCompatibilityVersion version4_1 = new BackwardCompatibilityVersion("4.1");
    BackwardCompatibilityVersion version4_1_3 = new BackwardCompatibilityVersion("4.1.3");

    @Test
    public void testMatch() {
        boolean match = version4_1_3.match(new BackwardCompatibilityVersion("4.1.3"));
        assertTrue(match);

        match = version4_1_3.match(new BackwardCompatibilityVersion("4.1"));
        assertFalse(match);
    }

    @Test
    public void testGreaterOrEqualThan() {
        boolean greaterOrEqual = version4_1.greaterOrEqualThan("4.1.3");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("4.1.2");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("4.1.4");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("4.2.1");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("4.0.1");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("5.1.3");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("3.1.3");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("4.1");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("4.2");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("4.0");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("5.1");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("3.1");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("4");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("3");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1.greaterOrEqualThan("5");
        assertFalse(greaterOrEqual);
    }

    @Test
    public void testGreaterOrEqualThanWithPatch() {
        boolean greaterOrEqual = version4_1_3.greaterOrEqualThan("4.1.3");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("4.1.2");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("4.1.4");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("5.1.3");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("3.1.3");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("4.1");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("4.2");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("4.0");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("5.1");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("3.1");
        assertFalse(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("4");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("3");
        assertTrue(greaterOrEqual);

        greaterOrEqual = version4_1_3.greaterOrEqualThan("5");
        assertFalse(greaterOrEqual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooManyVersionNumbersInVersionClass() {
        boolean greaterOrEqual = version4_1_3.greaterOrEqualThan("4.1.3.5");
        assertTrue(greaterOrEqual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooFewVersionNumbersInVersionClass() {
        boolean greaterOrEqual = version4_1_3.greaterOrEqualThan("");
        assertTrue(greaterOrEqual);
    }
}