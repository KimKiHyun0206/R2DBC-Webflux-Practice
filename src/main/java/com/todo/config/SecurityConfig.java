package com.todo.config;

import com.todo.jwt.JwtAuthenticationFilter;
import com.todo.jwt.JwtTokenProvider;
import com.todo.user.service.ReactiveUserDetailsServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.ReactiveAuthenticationManager;
import org.springframework.security.authentication.UserDetailsRepositoryReactiveAuthenticationManager;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.context.NoOpServerSecurityContextRepository;


@Configuration
@EnableWebFluxSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtTokenProvider tokenProvider;
    private final ReactiveUserDetailsServiceImpl reactiveUserDetailsService;

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                // JWT를 사용하기 때문에 CSRF를 비활성화한다.
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)

                /*
                Spring Security의 요청 캐싱 기능을 비활성화합니다.
                이는 인증되지 않은 사용자가 보호된 리소스에 접근했을 때,
                로그인 성공 후 원래 요청했던 페이지로 자동으로 리다이렉션하는 기능을 끕니다.
                API 서버와 같이 리다이렉션이 필요 없는 경우에 사용됩니다.
                */
                .requestCache(ServerHttpSecurity.RequestCacheSpec::disable)

                // 인증에 따른 접근 가능 여부 설정
                .authorizeExchange(exchanges -> exchanges
                        // Spring Actuator 접근 URL 허용
                        .pathMatchers("/actuator/**").permitAll()
                        // Swagger 접근에 필요한 URL
                        .pathMatchers(
                                "/v3/api-docs/**",
                                "/swagger-ui/**",
                                "/swagger-ui.html",
                                "/webjars/swagger-ui/**"
                        ).permitAll()
                        // 테스트용 API 및 로그인 API
                        .pathMatchers(
                                "/api/auth/login",
                                "/api/users/**",
                                "/api/posts/**",
                                "/api/image/**",
                                "/api/gemini/**",
                                "/api/hello",
                                "/api/delayed-response"
                        ).permitAll()
                        // 위에 명시된 경로를 제외한 모든 요청은 인증된 사용자만 접근할 수 있다.
                        .anyExchange().authenticated()
                )

                // JWT를 사용한 세션리스(stateless) 인증이므로, 세션 저장소를 사용하지 않도록 설정한다.
                .securityContextRepository(NoOpServerSecurityContextRepository.getInstance())

                // SecurityWebFiltersOrder.AUTHENTICATION 위치에 JwtAuthenticationFilter를 추가한다.
                // 이 필터는 HTTP 요청 헤더에서 JWT 토큰을 추출하고, 토큰의 유효성을 검사하여 인증 객체(Authentication)를 생성한다.
                .addFilterAt(new JwtAuthenticationFilter(tokenProvider), SecurityWebFiltersOrder.AUTHENTICATION)
                .build();
    }

    /**
     * 리액티브 환경에서 사용자 인증을 위한 매니저를 설정.
     * UserDetailsRepositoryReactiveAuthenticationManager를 사용하여,
     * ReactiveUserDetailsService로 사용자 정보를 조회하고, PasswordEncoder로 비밀번호를 검증한다.
     */
    @Bean
    public ReactiveAuthenticationManager authenticationManager() {
        UserDetailsRepositoryReactiveAuthenticationManager authenticationManager =
                new UserDetailsRepositoryReactiveAuthenticationManager(reactiveUserDetailsService);
        authenticationManager.setPasswordEncoder(passwordEncoder());
        return authenticationManager;
    }

    /**
     * 비밀번호를 안전하게 해시하기 위한 BCryptPasswordEncoder를 빈으로 등록한다.
     * BCrypt는 해시된 결과에 솔트(salt)를 포함하여 저장하므로, 무차별 대입 공격(brute-force attack)에 효과적으로 대응할 수 있다.
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}