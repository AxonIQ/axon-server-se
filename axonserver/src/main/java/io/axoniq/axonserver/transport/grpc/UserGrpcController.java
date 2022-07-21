/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.admin.user.api.UserAdminService;
import io.axoniq.axonserver.admin.user.api.UserRole;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.admin.CreateOrUpdateUserRequest;
import io.axoniq.axonserver.grpc.admin.DeleteUserRequest;
import io.axoniq.axonserver.grpc.admin.UserAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.UserOverview;
import io.axoniq.axonserver.grpc.admin.UserRoleOverview;
import io.axoniq.axonserver.grpc.admin.UserRoleRequest;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Controller;

import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * A gRPC controller for the UserAdminService. Controls Axon Server users via gRPC interface.
 *
 * @author Stefan Dragisic
 * @since 4.6.0
 */

@Controller
public class UserGrpcController extends UserAdminServiceGrpc.UserAdminServiceImplBase
        implements AxonServerClientService {

    private final UserAdminService userAdminService;
    private final AuthenticationProvider authenticationProvider;

    public UserGrpcController(UserAdminService userAdminService,
                              AuthenticationProvider authenticationProvider) {
        this.userAdminService = userAdminService;
        this.authenticationProvider = authenticationProvider;
    }

    private static UserRole toUserRole(UserRoleRequest role) {
        return new UserRole() {
            @Nonnull
            @Override
            public String context() {
                return role.getContext();
            }

            @Nonnull
            @Override
            public String role() {
                return role.getRole();
            }
        };
    }

    @Override
    public void createOrUpdateUser(CreateOrUpdateUserRequest request, StreamObserver<Empty> responseObserver) {
        try {
            userAdminService.createOrUpdateUser(request.getUserName(),
                                                request.getPassword(),
                                                request.getUserRolesList()
                                                       .stream()
                                                       .map(UserGrpcController::toUserRole)
                                                       .collect(Collectors.toSet()),
                                                new GrpcAuthentication(authenticationProvider));

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(GrpcExceptionBuilder.build(e));
        }
    }

    @Override
    public void deleteUser(DeleteUserRequest request, StreamObserver<Empty> responseObserver) {
        try {
            userAdminService.deleteUser(request.getUserName(), new GrpcAuthentication(authenticationProvider));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(GrpcExceptionBuilder.build(e));
        }
    }

    @Override
    public void getUsers(Empty request, StreamObserver<UserOverview> responseObserver) {
        try {
            userAdminService.users(new GrpcAuthentication(authenticationProvider))
                            .stream()
                            .map(user -> UserOverview.newBuilder()
                                                     .setUserName(user.getUserName())
                                                     .setEnabled(user.isEnabled())
                                                     .addAllUserRoles(user.getRoles()
                                                                          .stream()
                                                                          .map(userRole -> UserRoleOverview.newBuilder()
                                                                                                           .setContext(
                                                                                                                   userRole.getContext())
                                                                                                           .setRole(
                                                                                                                   userRole.getRole())
                                                                                                           .build())
                                                                          .collect(Collectors.toList()))
                                                     .build())
                            .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(GrpcExceptionBuilder.build(e));
        }
    }
}
