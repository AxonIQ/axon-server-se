package io.axoniq.axonserver.cluster.jpa;

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
 * @author Marc Gathier
 */
@RunWith(SpringRunner.class)
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
        testSubject.sync();
        assertEquals("me", testSubject.votedFor());
        assertEquals("me", raftStateRepository.getOne("SampleGroup").getVotedFor());
    }

    @Test
    public void updateCurrentTerm() {
        testSubject.updateCurrentTerm(1);
        testSubject.sync();
        assertEquals(1, testSubject.currentTerm());
        assertEquals(1, raftStateRepository.getOne("SampleGroup").getCurrentTerm());
    }

    @Test
    public void updateLastApplied()  {
        testSubject.updateLastApplied(100, 1);
        assertEquals(100, testSubject.lastAppliedIndex());
        testSubject.sync();
    }

    @Test
    public void updateCommitIndex()  {
        testSubject.updateCommit(100, 1);
        assertEquals(100, testSubject.commitIndex());
        testSubject.sync();
    }
}