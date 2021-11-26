package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.access.jpa.Role;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.roles.RoleController;
import io.axoniq.axonserver.access.user.UserControllerFacade;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.admin.CreateOrUpdateUserRequest;
import io.axoniq.axonserver.grpc.admin.DeleteUserRequest;
import io.axoniq.axonserver.grpc.admin.UserAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.UserOverview;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Controller;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Stefan Dragisic
 * @since 4.6.0
 */

@Controller
public class UserGrpcController extends UserAdminServiceGrpc.UserAdminServiceImplBase
        implements AxonServerClientService {

    private final UserControllerFacade userController;
    private final RoleController roleController;

    public UserGrpcController(UserControllerFacade userController, RoleController roleController) {
        this.userController = userController;
        this.roleController = roleController;
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

            userController.updateUser(request.getUserName(), request.getPassword(), request.getUserRolesList()
                    .stream()
                    .map(role -> new UserRole(role.getContext(), role.getRole()))
                    .collect(Collectors.toSet()));

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void deleteUser(DeleteUserRequest request, StreamObserver<Empty> responseObserver) {
        try {
            userController.deleteUser(request.getUserName());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getUsers(Empty request, StreamObserver<UserOverview> responseObserver) {
        try {
            userController.getUsers()
                    .stream()
                    .map(user -> UserOverview.newBuilder()
                            .setUserName(user.getUserName())
                            .setEnabled(user.isEnabled())
                            .build())
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
