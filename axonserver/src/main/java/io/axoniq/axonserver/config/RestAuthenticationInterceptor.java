package io.axoniq.axonserver.config;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Interceptor to authenticate REST requests when access control is enabled.
 *
 * @author Marc Gathier
 */
public class RestAuthenticationInterceptor implements HandlerInterceptor {

    private final AxonServerAccessController accessController;

    public RestAuthenticationInterceptor(AxonServerAccessController accessController) {
        this.accessController = accessController;
    }

    /**
     * Checks authentication based on the supplied token.
     * @param httpServletRequest  request message
     * @param httpServletResponse response message
     * @param handler - chosen handler to execute, for type and/or instance evaluation
     * @return true if processing of the request should continue.
     *
     * @throws IOException when it cannot write the exception to the response.
     */
    @Override
    public boolean preHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object handler)
            throws IOException {
        String token = StringUtils.getOrDefault(httpServletRequest.getHeader(AxonServerAccessController.TOKEN_PARAM),
                                                httpServletRequest.getParameter(AxonServerAccessController.TOKEN_PARAM));
        String context = StringUtils.getOrDefault(httpServletRequest.getHeader(AxonServerAccessController.CONTEXT_PARAM),
                                                  Topology.DEFAULT_CONTEXT);

        MessagingPlatformException exception = null;
        if (token == null) {
            exception = new MessagingPlatformException(ErrorCode.AUTHENTICATION_TOKEN_MISSING,
                                                       "Missing header: " + AxonServerAccessController.TOKEN_PARAM);
        } else if (!accessController.allowed(httpServletRequest.getMethod() + ":" + httpServletRequest.getRequestURI(),
                                             context,
                                             token)) {
            exception = new MessagingPlatformException(ErrorCode.AUTHENTICATION_INVALID_TOKEN, "Access denied");
        }

        if (exception != null) {
            httpServletResponse.setStatus(exception.getErrorCode().getHttpCode().value());
            httpServletResponse.addHeader("ErrorCode", String.valueOf(exception.getErrorCode().getCode()));
            httpServletResponse.getWriter().write(exception.getMessage());
            return false;
        }

        return true;
    }
}
