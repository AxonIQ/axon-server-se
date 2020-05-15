package io.axoniq.axonserver.enterprise.context;

import org.junit.*;

import static org.junit.Assert.*;

/**
 * Tests if the {@link ContextNameValidation} matches the correct names.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class ContextNameValidationTest {

    @Test
    public void correctName() {
        assertTrue(new ContextNameValidation().test("correctName_54"));
    }

    @Test
    public void allowAt() {
        assertTrue(new ContextNameValidation().test("correctName@54"));
    }

    @Test
    public void avoidStartWithNumber() {
        assertFalse(new ContextNameValidation().test("54name"));
    }

    @Test
    public void avoidStartWithAt() {
        assertFalse(new ContextNameValidation().test("@54name"));
    }

    @Test
    public void avoidStartWithUnderscore() {
        assertFalse(new ContextNameValidation().test("_name"));
    }

    @Test
    public void avoidComma() {
        assertFalse(new ContextNameValidation().test("name,54"));
    }

    @Test
    public void avoidSlash() {
        assertFalse(new ContextNameValidation().test("name/54"));
    }


}