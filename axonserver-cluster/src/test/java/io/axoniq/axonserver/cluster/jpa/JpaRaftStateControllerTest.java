package io.axoniq.axonserver.cluster.jpa;

import io.axoniq.axonserver.cluster.jpa.JpaRaftStateController;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static junit.framework.TestCase.assertEquals;

/**
 * Author: marc
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@SpringBootTest(classes = JpaRaftStateRepository.class)
@EnableAutoConfiguration
@EntityScan("io.axoniq.axonserver.cluster")
public class JpaRaftStateControllerTest {
    private JpaRaftStateController testSubject ;

    @Autowired
    private JpaRaftStateRepository raftStateRepository;

    @Before
    public void before(){
        testSubject = new JpaRaftStateController("SampleGroup", raftStateRepository);
        testSubject.init();
    }

    @Test
    public void markVotedFor() {
        testSubject.markVotedFor("me");
        assertEquals("me", testSubject.votedFor());
        assertEquals("me", raftStateRepository.getOne("SampleGroup").getVotedFor());
    }

    @Test
    public void updateCurrentTerm() {
        testSubject.updateCurrentTerm(1);
        assertEquals(1, testSubject.currentTerm());
        assertEquals(1, raftStateRepository.getOne("SampleGroup").getCurrentTerm());
    }

    @Test
    public void updateLastApplied()  {
        testSubject.updateLastApplied(100);
        assertEquals(100, testSubject.lastApplied());
        testSubject.sync();
    }

    @Test
    public void updateCommitIndex()  {
        testSubject.updateCommitIndex(100);
        assertEquals(100, testSubject.commitIndex());
        testSubject.sync();
    }
}