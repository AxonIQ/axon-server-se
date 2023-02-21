/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.admin.user.requestprocessor.UserController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.admin.ApplicationRoles;
import io.axoniq.axonserver.grpc.admin.AuthenticateUserRequest;
import io.axoniq.axonserver.grpc.admin.AuthenticationServiceGrpc;
import io.axoniq.axonserver.grpc.admin.ContextRole;
import io.axoniq.axonserver.grpc.admin.Token;
import io.axoniq.axonserver.grpc.admin.UserRoles;
import io.grpc.stub.StreamObserver;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;

@Controller
public class AuthenticationGrpcController
        extends AuthenticationServiceGrpc.AuthenticationServiceImplBase
        implements AxonServerClientService {

    private final UserController userController;
    private final AxonServerAccessController accessController;
    private final PasswordEncoder passwordEncoder;

    public AuthenticationGrpcController(UserController userController, AxonServerAccessController accessController,
                                        PasswordEncoder passwordEncoder) {
        this.userController = userController;
        this.accessController = accessController;
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public void authenticateUser(AuthenticateUserRequest request, StreamObserver<UserRoles> responseObserver) {
        User user = userController.findUser(request.getUserName());
        if (user == null) {
            responseObserver.onError(GrpcExceptionBuilder.build(new MessagingPlatformException(ErrorCode.AUTHENTICATION_INVALID_TOKEN,
                                                                                               "Invalid login")));
            return;
        }

        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            responseObserver.onError(GrpcExceptionBuilder.build(new MessagingPlatformException(ErrorCode.AUTHENTICATION_INVALID_TOKEN,
                                                                                               "Invalid login")));
            return;
        }

        UserRoles.Builder userOverview = UserRoles.newBuilder()
                                                  .setUserName(user.getUserName());

        user.getRoles().forEach(role -> userOverview.addUserRoles(contextRole(role.getRole(), role.getContext())));
        responseObserver.onNext(userOverview.build());
        responseObserver.onCompleted();
    }

    private ContextRole contextRole(String role, String context) {
        return ContextRole.newBuilder()
                          .setRole(role)
                          .setContext(context)
                          .build();
    }

    @Override
    public void authenticateToken(Token request, StreamObserver<ApplicationRoles> responseObserver) {
        try {
            Authentication authentication = accessController.authenticate(request.getToken());
            ApplicationRoles.Builder builder = ApplicationRoles.newBuilder()
                                                               .setApplicationName(authentication.getName());
            authentication.getAuthorities().forEach(authority -> builder.addApplicationRole(contextRole(authority)));

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    private ContextRole contextRole(GrantedAuthority authority) {
        String[] parts = authority.getAuthority().split("@", 2);
        if (parts.length == 2) {
            return contextRole(parts[0], parts[1]);
        }
        return contextRole(parts[0], accessController.defaultContextForRest());
    }
}
