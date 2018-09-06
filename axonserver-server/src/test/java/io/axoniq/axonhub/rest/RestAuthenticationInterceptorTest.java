package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.AxonHubAccessController;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class RestAuthenticationInterceptorTest {
    private RestAuthenticationInterceptor testSubject;
    @Mock
    private AxonHubAccessController accessController;

    @Before
    public void setUp() throws Exception {
        when(accessController.allowed(any(), any(), eq("12345"))).thenReturn(true);
        testSubject = new RestAuthenticationInterceptor(accessController);
    }

    @Test
    public void preHandleWithoutToken() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/v1/queries");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertFalse(testSubject.preHandle(request, response, null));
        assertEquals(403, response.getStatus());
        assertEquals("AXONIQ-1000", response.getHeader("AxonIQ-ErrorCode"));
    }

    @Test
    public void preHandleWithInvalidToken() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/v1/queries");
        request.addHeader("axoniq-access-token", "1234");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertFalse(testSubject.preHandle(request, response, null));
        assertEquals(403, response.getStatus());
        assertEquals("AXONIQ-1001", response.getHeader("AxonIQ-ErrorCode"));
    }

    @Test
    public void preHandleWithValidToken() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/v1/queries");
        request.addHeader("axoniq-access-token", "12345");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertTrue(testSubject.preHandle(request, response, null));
    }

    @Test
    public void preHandleLocalRequest() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/v1/applications");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertTrue(testSubject.preHandle(request, response, null));
    }
    @Test
    public void preHandleNonLocalRequest() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/v1/applications");
        request.setRemoteAddr("193.1.2.14");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertFalse(testSubject.preHandle(request, response, null));
    }
}