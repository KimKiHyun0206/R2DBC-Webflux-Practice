package com.todo.user.service;

import com.todo.exception.CustomException;
import com.todo.exception.dto.ErrorMessage;
import com.todo.user.domain.User;
import com.todo.user.dto.request.UserCreateRequest;
import com.todo.user.dto.request.UserUpdateRequest;
import com.todo.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final R2dbcEntityTemplate template;
    private final PasswordEncoder passwordEncoder;

    /**
     * 사용자 생성을 위한 메소드
     */
    @Transactional
    public Mono<User> create(UserCreateRequest user) {
        User entity = User.toEntity(user);
        entity.setPassword(passwordEncoder.encode(entity.getPassword()));
        return userRepository.save(entity)
                .doOnNext(result -> log.info("사용자({}) 생성 완료.", result.getName()));
    }

    /**
     * 사용자 조회를 위한 메소드
     *
     * @param id 조회할 사용자의 PK
     */
    @Transactional(readOnly = true)
    public Mono<User> read(Long id) {
        return userRepository.findById(id)
                .switchIfEmpty(
                        Mono.error(new CustomException(
                                ErrorMessage.NOT_FOUND_USER,
                                String.format("사용자(%d)를 찾지 못했습니다", id)
                        ))
                )
                .doOnNext(user -> log.info("사용자({}) 조회 완료", user.getId()));
    }

    /**
     * 모든 사용자를 조회하기 위한 메소드
     */
    @Transactional(readOnly = true)
    public Flux<User> readAll() {
        return userRepository.findAll()
                .doOnComplete(() -> log.info("모든 사용자 조회 완료."));
    }

    /**
     * 이메일로 사용자를 조회하기 위한 메소드
     */
    @Transactional(readOnly = true)
    public Mono<User> getUserByEmail(String email) {
        return userRepository.findByEmail(email).switchIfEmpty(
                        Mono.error(new CustomException(
                                ErrorMessage.NOT_FOUND_USER,
                                String.format("이메일(%s)를 사용하는 사용자를 찾지 못했습니다", email)
                        ))
                )
                .doOnNext(user -> log.info("이메일로 사용자({}) 조회 완료.", user.getEmail()));
    }

    /*
     * - 세밀한 제어가 필요한 경우 사용하기 좋다.
     * - Query와 Update 객체를 조합하여 특정 조건에 맞는 데이터만 부분적으로 업데이트할 수 있다.
     * - 이 방법은 엔티티 객체 전체를 불러와 수정하는 대신, 필요한 필드만 효율적으로 업데이트할 수 있다.
     * */
    @Transactional
    public Mono<Long> updateUsingTemplate(Long id, UserUpdateRequest request) {
        // 사용해 사용자의 존재 여부를 확인합니다.
        return userRepository.existsById(id)
                .flatMap(exists -> {
                    if (!exists) {
                        // 존재하지 않으면 CustomException을 발생시킵니다.
                        return Mono.error(new CustomException(
                                ErrorMessage.NOT_FOUND_USER,
                                String.format("사용자(%d)를 찾지 못했습니다", id)
                        ));
                    }

                    // 존재하면 업데이트 로직을 실행하는 Mono를 반환합니다.
                    Update update = Update
                            .update("name", request.name() == null ? "홍길동" : request.name())
                            .set("email", request.email() == null ? "example@example.com" : request.email());

                    return template.update(
                                    Query.query(Criteria.where("id").is(String.valueOf(id))),
                                    update,
                                    User.class
                            ).filter(count -> count == 1)
                            .switchIfEmpty(Mono.error(new CustomException(
                                    ErrorMessage.INTERNAL_SERVER_ERROR,
                                    "사용자 수정 중 여러 행이 수정되었습니다.")
                            ));
                })
                .doOnNext(l -> log.info("{}행 업데이트 완료.", l));
    }

    /*
     * - Spring MVC의 더티 채킹 비슷하게 하는 방법이다(코드가 비슷하지 내부 동작이 비슷한 건 아니다)
     * - 엔티티를 조회한 다음 수정한 다음에 PK 값에 의존하여 update하는 방법이다.
     * */
    @Transactional
    public Mono<User> updateUsingRepository(Long id, UserUpdateRequest request) {
        return userRepository.findById(id)
                .switchIfEmpty(Mono.error(new CustomException(
                        ErrorMessage.NOT_FOUND_USER,
                        String.format("사용자(%d)를 찾지 못했습니다", id)
                )))
                .flatMap(user -> {
                    user.setName(request.name() == null ? "홍길동" : request.name());
                    user.setEmail(request.email() == null ? "example@example.com" : request.email());
                    return userRepository.save(user);
                })
                .doOnNext(user -> log.info("사용자({}) 레포지토리를 사용해 수정 완료.", user.getId()));
    }

    /**
     * ID로 사용자를 삭제하기 위한 메소드
     */
    @Transactional
    public Mono<Void> deleteUsingRepository(Long id) {
        return userRepository.findById(id)
                .switchIfEmpty(Mono.error(new CustomException(
                        ErrorMessage.NOT_FOUND_USER,
                        String.format("사용자(%d)를 찾지 못했습니다", id)
                )))
                .flatMap(userRepository::delete)
                .doOnSuccess(v -> log.info("사용자({}) 레포지토리로 삭제 완료.", id));
    }

    /*
     * - 복잡한 조건을 사용하여 데이터를 삭제할 때 사용한다.
     * - 특정 필드 값을 기준으로 여러 데이터를 한 번에 삭제해야 할 때 유용하다.
     * - 하지만 이번에는 여러 값을 삭제하는 것이 아닌 예시 용으로 단건 삭제만 구현했다.
     * */
    @Transactional
    public Mono<Long> deleteUsingTemplate(Long id) {
        return template.delete(
                        Query.query(Criteria.where("id").is(String.valueOf(id))),
                        User.class
                )
                .flatMap(count -> {
                    if (count == 0) {
                        return Mono.error(new CustomException(
                                ErrorMessage.NOT_FOUND_USER,
                                String.format("사용자(%d)를 찾지 못했습니다", id)
                        ));
                    }
                    return Mono.just(count);
                })
                .doOnNext(l -> log.info("사용자({}) 템플릿으로 삭제 완료.", id));
    }
}