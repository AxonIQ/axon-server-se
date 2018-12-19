package io.axoniq.platform.user;

import io.axoniq.platform.application.ApplicationModelController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Author: marc
 */
@Controller
public class UserController {
    private final Logger logger = LoggerFactory.getLogger(UserController.class);
    private final PasswordEncoder passwordEncoder;
    private final ApplicationModelController applicationModelController;
    private final UserRepository userRepository;
    private final Map<String, Consumer<User>> updateListeners = new ConcurrentHashMap<>();
    private final Map<String, Consumer<String>> deleteListeners = new ConcurrentHashMap<>();


    public UserController(PasswordEncoder passwordEncoder, ApplicationModelController applicationModelController, UserRepository userRepository) {
        this.passwordEncoder = passwordEncoder;
        this.applicationModelController = applicationModelController;
        this.userRepository = userRepository;
    }

    @Transactional
    public void deleteUser(String username) {
        synchronized (userRepository) {
            userRepository.findById(username).ifPresent(u -> {
                userRepository.delete(u);
                userRepository.flush();
            });
        }
        deleteListeners.forEach((k,v)-> v.accept(username));
    }

    public List<User> getUsers() {
        return userRepository.findAll();
    }

    public void registerUpdateListener(String name, Consumer<User> updateListener) {
        updateListeners.put(name, updateListener);
    }

    public void registerDeleteListener(String name, Consumer<String> deleteListener) {
        deleteListeners.put(name, deleteListener);
    }

    public void deregisterListeners(String name) {
        updateListeners.remove(name);
        deleteListeners.remove(name);
    }


    @Transactional
    public User syncUser(String username, String password, String[] roles) {
        synchronized (userRepository) {
            if (password == null) {
                password = userRepository.findById(username).map(User::getPassword).orElse(null);
            }
            User user = userRepository.save(new User(username, password, roles));
            userRepository.flush();
            return user;
        }
    }

    public User updateUser(String username, String password, String[] roles) {
        User user = syncUser(username, password == null ? null: passwordEncoder.encode(password), roles);
        updateListeners.forEach((k,v)-> v.accept(user));
        applicationModelController.incrementModelVersion(User.class);
        return user;
    }

    public void clearUsers() {
        userRepository.deleteAll();
    }

    public void syncUser(User jpaUser) {
        synchronized (userRepository) {
            userRepository.save(jpaUser);
        }
    }
}
