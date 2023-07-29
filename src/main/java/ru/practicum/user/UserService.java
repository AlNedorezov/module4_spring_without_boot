package ru.practicum.user;

import java.util.List;

interface UserService {
    List<UserDto> getAllUsers();
    UserDto getById(Long id);
    UserDto saveUser(UserDto userDto);
}