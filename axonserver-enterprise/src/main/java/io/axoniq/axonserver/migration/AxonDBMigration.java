package io.axoniq.axonserver.migration;

import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.access.application.JpaApplication;
import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.jpa.Safepoint;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.persistence.EntityManager;

/**
 * @author Marc Gathier
 */
@Component
@ConditionalOnProperty("axoniq.axonserver.axondb.datasource")
public class AxonDBMigration implements SmartLifecycle {
    private final Logger logger = LoggerFactory.getLogger(AxonDBMigration.class);
    private volatile boolean running;
    private final String datasource;
    private final String username;
    private final String password;
    private final EmbeddedDBProperties embeddedDBProperties;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final EntityManager entityManager;
    private final LifecycleController lifecycleController;

    public AxonDBMigration(@Value("${axoniq.axonserver.axondb.datasource}") String datasource,
                           @Value("${axoniq.axonserver.axondb.username:sa}") String username,
                           @Value("${axoniq.axonserver.axondb.password:}") String password,
                           EmbeddedDBProperties embeddedDBProperties,
                           MessagingPlatformConfiguration messagingPlatformConfiguration,
                           EntityManager entityManager, LifecycleController lifecycleController) {
        this.datasource = datasource;
        this.username = username;
        this.password = password;
        this.embeddedDBProperties = embeddedDBProperties;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.entityManager = entityManager;
        this.lifecycleController = lifecycleController;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(@NonNull Runnable callback) {
        callback.run();

    }

    @Override
    @Transactional
    public void start() {
        logger.warn("Starting AxonDBMigration from: {}", datasource);
        if( checkEmptyControldb() ) {
            try (Connection connection = DriverManager.getConnection(datasource, username, password)) {
                checkCurrentNode(connection);
                checkExistingDatafiles(connection);

                    migrateApplications(connection);
                    migrateUsers(connection);
                    migrateNodes(connection);
                    migrateContexts(connection);
                    migrateSafepoints(connection);

            } catch (Exception ex) {
                logger.warn("Exception during AxonDB migration", ex);
                lifecycleController.abort();
            }
        }

        running = true;
    }

    private void migrateContexts(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("SELECT CONTEXT.CONTEXT, CONTEXT_CLUSTER_NODE.CLUSTER_NODE_NAME "
                                                                        + "FROM CONTEXT, CONTEXT_CLUSTER_NODE "
                                                                        + "WHERE CONTEXT.ID = CONTEXT_CLUSTER_NODE.CONTEXT_ID");
        ResultSet resultSet = ps.executeQuery()) {
            while(resultSet.next()) {
                String contextName = resultSet.getString(1);
                String node = resultSet.getString(2);
                Context context = entityManager.find(Context.class, contextName);
                if( context == null) {
                    context = new Context(contextName);
                    entityManager.persist(context);
                    entityManager.flush();
                }

                ClusterNode clusterNode = entityManager.find(ClusterNode.class, node);
                clusterNode.addContext(context, clusterNode.getName());
            }

        }
    }

    private void migrateNodes(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("SELECT NAME, HOST_NAME, INTERNAL_HOST_NAME, GRPC_PORT, GRPC_INTERNAL_PORT, HTTP_PORT "
                                                                        + "FROM CLUSTER_NODE");
        ResultSet rs = ps.executeQuery()) {
            while( rs.next()) {
                ClusterNode clusterNode = new ClusterNode(rs.getString(1), rs.getString(2), rs.getString(3),
                                                          rs.getInt(4), rs.getInt(5),rs.getInt(6));
                logger.warn("Create node: {}", clusterNode.getName());
                    entityManager.persist(clusterNode);
                    entityManager.flush();
            }
        }
    }

    private void migrateUsers(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("SELECT USERS.USERNAME, USERS.PASSWORD, USERS.ENABLED, USER_ROLES.ROLE "
                                                                        + "        FROM USERS, USER_ROLES "
                                                                        + "        WHERE USERS.USERNAME = USER_ROLES.USERNAME "
                                                                        + "        ORDER BY USERS.USERNAME");
            ResultSet rs = ps.executeQuery())  {
            User currentUser = null;
            while(rs.next()) {
                String userName = rs.getString(1);
                String role = rs.getString(4);
                if(currentUser == null || !userName.equals(currentUser.getUserName())) {
                    if( currentUser != null) {
                        entityManager.persist(currentUser);
                    }
                    currentUser = new User(userName, rs.getString(2));
                    currentUser.setEnabled(rs.getBoolean(3));
                }
//                if( ! role.equals("USER")) {
//                    currentUser.addRole(rs.getString(4));
//                }
            }
            if( currentUser != null) {
                entityManager.persist(currentUser);
            }
        }


    }

    private void migrateApplications(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "select application.name, application.description, application.hashed_token, application_role.context, application_role.role"+
            " from application, application_roles, application_role" +
            " where application.id = application_roles.application_id" +
            " and application_role.id = application_roles.roles_id");
             ResultSet resultSet = ps.executeQuery() ) {
            JpaApplication application = null;
            while(resultSet.next()) {
                String applicationName = resultSet.getString(1);
                if( application == null || ! applicationName.equals(application.getName())) {
                    if( application != null) {
                        entityManager.persist(application);
                    }
                    application = new JpaApplication(applicationName, resultSet.getString(2), null, resultSet.getString(3));
                }

                application.addRole(resultSet.getString(4), resultSet.getString(5));
            }
        }

    }

    private void migrateSafepoints(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("SELECT CONTEXT, TYPE, TOKEN "
                                                                        + "FROM SAFEPOINT");
             ResultSet rs = ps.executeQuery())  {
            while( rs.next()) {
                String context = rs.getString(1);
                String type = rs.getString(2);
                long token = rs.getLong(3);
                logger.warn("{}: Storing safepoint: {} = {}", context, type, token);
                Safepoint safepoint = new Safepoint(type.toUpperCase(), context, token, 0);
                entityManager.persist(safepoint);
            }
        }
    }

    private void checkExistingDatafiles(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("SELECT CONTEXT.CONTEXT "
                                            + "FROM CONTEXT, CONTEXT_CLUSTER_NODE "
                                            + "WHERE CONTEXT.ID = CONTEXT_CLUSTER_NODE.CONTEXT_ID "
                                            + "AND CONTEXT_CLUSTER_NODE.CLUSTER_NODE_NAME = ?") ) {
            ps.setString(1, messagingPlatformConfiguration.getName());
            try(ResultSet rs = ps.executeQuery()) {
                while( rs.next()) {
                    String context = rs.getString(1);
                    File eventFile = embeddedDBProperties.getEvent().dataFile(context, 0);
                    if( ! eventFile.exists()) {
                        throw new RuntimeException(String.format("Events file %s not found", eventFile.getAbsolutePath()));
                    }
                    File snapshotFile = embeddedDBProperties.getSnapshot().dataFile(context, 0);
                    if( ! snapshotFile.exists()) {
                        throw new RuntimeException(String.format("Snapshots file %s not found", snapshotFile.getAbsolutePath()));
                    }
                }
            }
        }

    }

    private void checkCurrentNode(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("SELECT HOST_NAME, INTERNAL_HOST_NAME, GRPC_PORT, GRPC_INTERNAL_PORT, HTTP_PORT "
                                                                        + "FROM CLUSTER_NODE WHERE NAME = ?") ) {
            ps.setString(1, messagingPlatformConfiguration.getName());
            try(ResultSet rs = ps.executeQuery()) {
                if( rs.next()) {
                    String hostName = rs.getString(1);
                    if( ! hostName.equals(messagingPlatformConfiguration.getFullyQualifiedHostname())) {
                        throw new RuntimeException(String.format("Mismatch in hostnames, %s in AxonDB, %s in axonserver.properties",
                                                                 hostName, messagingPlatformConfiguration.getFullyQualifiedHostname()));
                    }
                    String internalHostName = rs.getString(2);
                    if( ! internalHostName.equals(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())) {
                        throw new RuntimeException(String.format("Mismatch in internal-hostname, %s in AxonDB, %s in axonserver.properties",
                                                                 internalHostName, messagingPlatformConfiguration.getFullyQualifiedInternalHostname()));
                    }

                    int port = rs.getInt(3);
                    if( port != messagingPlatformConfiguration.getPort()) {
                        throw new RuntimeException(String.format("Mismatch in port, %s in AxonDB, %s in axonserver.properties",
                                                                 port, messagingPlatformConfiguration.getPort()));
                    }
                    int internalPort = rs.getInt(4);
                    if( internalPort!= messagingPlatformConfiguration.getInternalPort()) {
                        throw new RuntimeException(String.format("Mismatch in internal-port, %s in AxonDB, %s in axonserver.properties",
                                                                 internalPort, messagingPlatformConfiguration.getInternalPort()));
                    }
                    int httpPort = rs.getInt(5);
                    if( httpPort != messagingPlatformConfiguration.getHttpPort()) {
                        throw new RuntimeException(String.format("Mismatch in server.port, %s in AxonDB, %s in axonserver.properties",
                                                                 httpPort, messagingPlatformConfiguration.getHttpPort()));
                    }
                } else {
                    throw new RuntimeException(String.format("Node name %s not found in AxonDB database",  messagingPlatformConfiguration.getName()));
                }
            }
        }

    }

    private boolean checkEmptyControldb() {
        long safepointCount = entityManager.createQuery("select count(s) from Safepoint s", Long.class).getSingleResult();
        if( safepointCount > 0) {
            logger.warn("Data found in controlDB, not performing migration");
            return false;
        }

        return true;
    }

    @Override
    public void stop() {
        stop(() -> {});
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return -1000;
    }
}
