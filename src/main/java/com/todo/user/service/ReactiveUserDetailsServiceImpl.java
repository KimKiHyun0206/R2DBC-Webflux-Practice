package com.todo.user.service;

import com.todo.user.domain.User;
import com.todo.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.userdetails.ReactiveUserDetailsService;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ReactiveUserDetailsServiceImpl implements ReactiveUserDetailsService {
    private final UserRepository userRepository;

    @Override
    public Mono<UserDetails> findByUsername(String username) {
        return userRepository.findByUsername(username)
                .map(this::convertUserToUserDetails);
    }


    // User 엔티티를 UserDetails로 변환하는 헬퍼 메서드
    private UserDetails convertUserToUserDetails(User user) {
        return org.springframework.security.core.userdetails.User
                .withUsername(user.getUsername())
                .password(user.getPassword()) // 이미 인코딩된 비밀번호
                .roles(user.getRoles().split(","))
                .build();
    }
}