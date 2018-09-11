package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.GrpcMetadataKeys;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static io.axoniq.axonserver.AxonServerAccessController.TOKEN_PARAM;

/**
 * Author: marc
 */
public class RestAuthenticationInterceptor implements HandlerInterceptor {
    private final AxonServerAccessController axonHubAccessController;

    public RestAuthenticationInterceptor(AxonServerAccessController axonHubAccessController) {
        this.axonHubAccessController = axonHubAccessController;
    }

    @Override
    public boolean preHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object handler) throws Exception {
        if( isLocalRequest(httpServletRequest)) return true;

        String token = httpServletRequest.getHeader(TOKEN_PARAM);
        if( token == null) {
            token = httpServletRequest.getParameter(TOKEN_PARAM);
        }

        String context = (String) httpServletRequest.getAttribute(AxonServerAccessController.CONTEXT_PARAM);
        if( context == null) context = ContextController.DEFAULT;

        MessagingPlatformException exception = null;
        if(token == null) {
            exception = new MessagingPlatformException(ErrorCode.AUTHENTICATION_TOKEN_MISSING, "Missing header: " + TOKEN_PARAM);
        } else if( ! axonHubAccessController.allowed(httpServletRequest.getMethod() + ":" + httpServletRequest.getRequestURI(), context, token) ) {
            exception = new MessagingPlatformException(ErrorCode.AUTHENTICATION_INVALID_TOKEN, "Access denied");
        }

        if( exception != null) {
            httpServletResponse.setStatus(exception.getErrorCode().getHttpCode().value());
            httpServletResponse.addHeader(GrpcMetadataKeys.ERROR_CODE_KEY.name(), String.valueOf(exception.getErrorCode().getCode()));
            httpServletResponse.getWriter().write(exception.getMessage());
            return false;

        }

        return true;
    }

    private boolean isLocalRequest(HttpServletRequest httpServletRequest) {
        if( httpServletRequest.getRequestURI().startsWith("/v1/applications")
                || httpServletRequest.getRequestURI().startsWith("/v1/users")
                || httpServletRequest.getRequestURI().startsWith("/v1/cluster")) {
            if( httpServletRequest.getLocalAddr().equals(httpServletRequest.getRemoteAddr())) {
                return true;
            }
        }
        return false;
    }


    @Override
    public void postHandle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object o, ModelAndView modelAndView)  {

    }

    @Override
    public void afterCompletion(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object o, Exception e)  {

    }
}
