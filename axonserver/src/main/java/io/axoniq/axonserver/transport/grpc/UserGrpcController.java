package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.admin.user.api.UserAdminService;
import io.axoniq.axonserver.admin.user.api.UserRole;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.admin.CreateOrUpdateUserRequest;
import io.axoniq.axonserver.grpc.admin.DeleteUserRequest;
import io.axoniq.axonserver.grpc.admin.UserAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.UserOverview;
import io.axoniq.axonserver.grpc.admin.UserRoleOverview;
import io.axoniq.axonserver.grpc.admin.UserRoleRequest;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Controller;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */

@Controller
public class UserGrpcController extends UserAdminServiceGrpc.UserAdminServiceImplBase implements AxonServerClientService {

    private final UserAdminService userAdminService;

    public UserGrpcController(UserAdminService userAdminService) {
        this.userAdminService = userAdminService;
    }

    @Override
    public void createOrUpdateUser(CreateOrUpdateUserRequest request, StreamObserver<Empty> responseObserver) {
        try {
            userAdminService.createOrUpdateUser(request.getUserName(), request.getPassword(), request
                    .getUserRolesList().stream().map(this::toUserRole).collect(Collectors.toSet()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void deleteUser(DeleteUserRequest request, StreamObserver<Empty> responseObserver) {
        try {
            userAdminService.deleteUser(request.getUserName());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getUsers(Empty request, StreamObserver<UserOverview> responseObserver) {
        try {
            userAdminService.users().stream().map(u -> UserOverview.newBuilder()
                    .setUserName(u.getUserName())
                    .setEnabled(u.isEnabled())
                    .addAllUserRoles(u.getRoles().stream().map(r -> UserRoleOverview
                            .newBuilder()
                            .setContext(r.getContext())
                            .setRole(r.getRole()).build()).collect(Collectors.toList()))
                    .build()).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Nonnull
    private UserRole toUserRole(UserRoleRequest r) {
        return new UserRole() {
            @Nonnull
            @Override
            public String role() {
                return r.getRole();
            }

            @Nonnull
            @Override
            public String context() {
                return r.getContext();
            }
        };
    }
}