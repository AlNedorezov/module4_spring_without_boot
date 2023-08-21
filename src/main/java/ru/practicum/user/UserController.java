package ru.practicum.user;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/users")
public class UserController {
    private final UserService userService;
    private final UserProducer userProducer;

    @GetMapping
    public List<UserDto> getAllUsers() {
        return userService.getAllUsers();
    }

    @GetMapping("/{userId}")
    public UserDto getAllUsers(@PathVariable final Long userId) {
        return userService.getById(userId);
    }

    @PostMapping
    public UserDto saveNewUser(@RequestBody UserDto user) {
        return userService.saveUser(user);
    }

    @PostMapping("/schedule-creation")
    public void scheduleSaveNewUser(@RequestBody UserDto user) {
        userProducer.send(user);
    }
}