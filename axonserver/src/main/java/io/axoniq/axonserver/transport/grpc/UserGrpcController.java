package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.access.jpa.Role;
import io.axoniq.axonserver.access.roles.RoleController;
import io.axoniq.axonserver.admin.user.api.UserAdminService;
import io.axoniq.axonserver.admin.user.api.UserRole;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.admin.CreateOrUpdateUserRequest;
import io.axoniq.axonserver.grpc.admin.DeleteUserRequest;
import io.axoniq.axonserver.grpc.admin.UserAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.UserOverview;
import io.axoniq.axonserver.grpc.admin.UserRoleRequest;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Controller;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A gRPC controller for the UserAdminService.
 * Controls Axon Server users via gRPC interface.
 *
 * @author Stefan Dragisic
 * @since 4.6.0
 */

@Controller
public class UserGrpcController extends UserAdminServiceGrpc.UserAdminServiceImplBase
        implements AxonServerClientService {

    private final UserAdminService userAdminService;
    private final RoleController roleController;

    public UserGrpcController(UserAdminService userAdminService, RoleController roleController) {
        this.userAdminService = userAdminService;
        this.roleController = roleController;
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
            Set<String> validRoles = roleController.listRoles().stream().map(Role::getRole).collect(Collectors.toSet());

            request.getUserRolesList()
                    .forEach(role -> {
                        if (!validRoles.contains(role.getRole())) {
                            responseObserver.onError(new MessagingPlatformException(ErrorCode.UNKNOWN_ROLE,
                                    role + ": Role unknown"));
                        }
                    });

            userAdminService.createOrUpdateUser(request.getUserName(), request.getPassword(), request.getUserRolesList()
                    .stream()
                    .map(UserGrpcController::toUserRole)
                    .collect(Collectors.toSet()));

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
            userAdminService.users()
                    .stream()
                    .map(user -> UserOverview.newBuilder().setUserName(user.getUserName()).setEnabled(user.isEnabled()).build())
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
