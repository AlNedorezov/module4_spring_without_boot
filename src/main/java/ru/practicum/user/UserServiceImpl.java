package ru.practicum.user;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
class UserServiceImpl implements UserService {
    private final UserRepository repository;
    private final UserRepositoryQDSL queryDSLRepository;

    @Override
    public List<UserDto> getAllUsers() {
        List<User> users = repository.findAll();
        return UserMapper.mapToUserDto(users);
    }

    public UserDto getById(Long id) {
        User user = queryDSLRepository.findById(id);
        return UserMapper.mapToUserDto(user);
    }

    @Override
    public UserDto saveUser(UserDto userDto) {
        User user = repository.save(UserMapper.mapToNewUser(userDto));
        return UserMapper.mapToUserDto(user);
    }
}