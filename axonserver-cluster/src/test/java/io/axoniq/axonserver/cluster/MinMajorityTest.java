package io.axoniq.axonserver.cluster;

import org.junit.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class MinMajorityTest {

    @Test
    public void getForTwo() {
        MinMajority testSubject = new MinMajority(() -> 2);
        assertEquals(2, (int)testSubject.get());
    }
    @Test
    public void getForThree() {
        MinMajority testSubject = new MinMajority(() -> 3);
        assertEquals(2, (int)testSubject.get());
    }
}
