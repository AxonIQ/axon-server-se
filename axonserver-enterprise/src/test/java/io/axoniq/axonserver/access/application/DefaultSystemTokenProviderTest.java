package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.LifecycleController;
import org.junit.*;
import org.junit.rules.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class DefaultSystemTokenProviderTest {

    private LifecycleController lifecycleController = mock(LifecycleController.class);
    private DefaultSystemTokenProvider testSubject;

    @Rule
    public TemporaryFolder tokenDir = new TemporaryFolder();

    @Test
    public void getFromGeneratedFile() {
        testSubject = new DefaultSystemTokenProvider(lifecycleController,
                                                     tokenDir.getRoot().getAbsolutePath(),
                                                     null,
                                                     true);
        testSubject.generateSystemToken();
        assertNotNull(testSubject.get());
        verify(lifecycleController, times(0)).abort();
    }

    @Test
    public void getFromGeneratedFileFailsToCreate() throws IOException {
        File fileInTheWay = tokenDir.newFile();
        testSubject = new DefaultSystemTokenProvider(lifecycleController, fileInTheWay.getAbsolutePath(), null, true);
        testSubject.generateSystemToken();
        assertNull(testSubject.get());
        verify(lifecycleController).abort();
    }

    @Test
    public void getFromSpecifiedFile() throws IOException {
        File tokenFile = tokenDir.newFile();
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tokenFile))) {
            bufferedWriter.write("this-is-the-token\n");
        }

        testSubject = new DefaultSystemTokenProvider(lifecycleController, null, tokenFile.getAbsolutePath(), true);
        testSubject.generateSystemToken();
        assertEquals("this-is-the-token", testSubject.get());
        verify(lifecycleController, times(0)).abort();
    }

    @Test
    public void getFromEmptyFileFails() throws IOException {
        File tokenFile = tokenDir.newFile();
        testSubject = new DefaultSystemTokenProvider(lifecycleController, null, tokenFile.getAbsolutePath(), true);
        testSubject.generateSystemToken();
        assertNull(testSubject.get());
        verify(lifecycleController).abort();
    }

    @Test
    public void getFromNonExistingFileFails() throws IOException {
        testSubject = new DefaultSystemTokenProvider(lifecycleController, null, UUID.randomUUID().toString(), true);
        testSubject.generateSystemToken();
        assertNull(testSubject.get());
        verify(lifecycleController).abort();
    }
}