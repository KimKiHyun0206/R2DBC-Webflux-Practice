package com.todo.controller;

import com.todo.exception.CustomException;
import com.todo.exception.dto.ErrorMessage;
import com.todo.image.ImagePathValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Paths;

@Slf4j
@RestController
@RequestMapping("/api/images")
@RequiredArgsConstructor
public class ImageSendController {

    private final ResourceLoader resourceLoader;
    private final ResourcePatternResolver resourcePatternResolver;
    private final ImagePathValidator imagePathValidator;
    private final String locationPattern = "classpath:img/";

    /**
     * Resource를 사용한 정적 이미지 전송
     */
    @GetMapping(produces = MediaType.IMAGE_PNG_VALUE)
    public Mono<Resource> getImage() {
        String imagePath = locationPattern + "webflux.png";

        // resourceLoader.getResource()는 존재하지 않는 파일도 Resource 객체를 반환하므로,
        // .exists()로 실제로 파일이 있는지 확인하는 것이 안전합니다.
        Resource imageResource = resourceLoader.getResource(imagePath);

        if (!imageResource.exists()) {
            // 파일을 찾지 못했을 경우 404 Not Found 에러를 반환
            return Mono.error(new CustomException(ErrorMessage.NOT_FOUND_IMAGE, "Image not found: " + imagePath));
        }

        return Mono.just(imageResource);
    }

    /**
     * /api/image/파일명 으로 URL을 받아서 이미지를 가져올 수 있도록 하는 REST API. 요청할 때 .png를 붙여야 한다. 안 그러면 오류남.
     *
     * @implNote 프로덕션 환경에서는 파일명이 아니라 데이터베이스에 조회 가능한 이미지의 ID 값을 받아서 조회해서 응답해야 한다. 프로덕션 환경이 아니고 간단히 구현하기 위해 이렇게 한 것.
     */
    @GetMapping(value = "/{image:.+\\.png}", produces = MediaType.IMAGE_PNG_VALUE)
    public Mono<Resource> getImage(@PathVariable String image) {
        String fileName = Paths.get(image).getFileName().toString();

        // 유효하지 않은 요청이라면 오류를 발생시킴
        if (!imagePathValidator.isValidPath(fileName)) {
            return Mono.error(new CustomException(
                    ErrorMessage.INVALID_IMAGE_PATH,
                    "유효한 이미지 요청이 아닙니다.")
            );
        }

        // resourceLoader.getResource()는 존재하지 않는 파일도 Resource 객체를 반환하므로,
        // .exists()로 실제로 파일이 있는지 확인하는 것이 안전합니다.
        Resource imageResource = resourceLoader.getResource(locationPattern + image);

        if (!imageResource.exists()) {
            // 파일을 찾지 못했을 경우 404 Not Found 에러를 반환
            return Mono.error(new CustomException(ErrorMessage.NOT_FOUND_IMAGE, "Image not found: " + fileName));
        }

        return Mono.just(imageResource);
    }

    /**
     * DataBuffer를 사용해서 스트림으로 데이터를 전송. 이미지 폴더에 있는 이미지를 모두 옥텟 스트림으로 보낸다.
     *
     * @implNote 프론트(클라이언트)에서 옥텟 스트리밍을 파싱할 수 있는 기능을 갖춰야한다.
     */
    @GetMapping(value = "/stream", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Flux<DataBuffer> streamImage() throws IOException {
        // ResourceLoader를 ResourcePatternResolver로 캐스팅하여 와일드카드를 지원하게 함
        Resource[] resources = resourcePatternResolver.getResources(locationPattern);

        // 찾은 모든 리소스를 Flux로 변환
        return Flux.fromArray(resources)
                .flatMap(resource -> {
                    // 각 리소스를 DataBuffer 스트림으로 읽어옴
                    return DataBufferUtils.read(
                            resource,
                            new DefaultDataBufferFactory(),
                            1024
                    );
                });
    }


    /**
     * 클라이언트에게 SSE(Server-Sent Events)를 통해 이미지 조회 URL 목록을 스트리밍한다.
     * <br>
     * {@code limitRate} 오퍼레이터를 사용하여 클라이언트의 처리 속도에 맞춰
     * 백프레셔(backpressure)를 적용하고 데이터 전송 속도를 제어한다.
     *
     * @return 이미지 조회 URL을 포함하는 {@link Flux<String>}
     * @implNote 클라이언트(브라우저)는 SSE를 통해 이 URL들을 수신한 후,
     * 개별적으로 이미지 콘텐츠를 요청하는 로직을 구현해야 합니다.
     */
    @GetMapping(value = "/flux/limit-rate", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getImageUrls() {
        // 예시 데이터: 실제로는 DB 조회나 파일 시스템 접근을 통해 동적으로 URL을 생성할 수 있다.
        return Flux.just(
                        "/api/images/webflux.png",
                        "/api/images/docker.png",
                        "/api/images/swagger.png"
                )
                .limitRate(2)                     // 클라이언트가 한 번에 최대 2개의 데이터를 요청하도록 백프레셔 적용
                .subscribeOn(Schedulers.boundedElastic());  // I/O 작업을 위한 별도 스레드에서 실행
    }

    /**
     * 클라이언트에게 SSE(Server-Sent Events)를 통해 이미지 조회 URL 목록을 스트리밍한다.
     * <p>
     * {@code onBackpressureBuffer} 오퍼레이터를 사용하여 생산자가 소비자보다 빠를 경우
     * 데이터를 버퍼에 저장하고, 버퍼가 가득 차면 가장 최신 데이터를 버려
     * 백프레셔(backpressure)를 처리한다.
     *
     * @return 이미지 조회 URL을 포함하는 {@link Flux<String>}
     * @implNote 클라이언트(브라우저)는 SSE를 통해 이 URL들을 수신한 후, 개별적으로 이미지 콘텐츠를 요청하는 로직을 구현해야 합니다.
     */
    @GetMapping(value = "/flux/buffer", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getImageUrlsBuffer() {
        // 예시 데이터: 실제로는 DB 조회나 파일 시스템 접근을 통해 동적으로 URL을 생성할 수 있습니다.
        return Flux.just(
                        "/api/images/webflux.png",
                        "/api/images/docker.png",
                        "/api/images/swagger.png"
                )
                // 버퍼 크기를 10으로 설정하고, 버퍼 오버플로우 시 최신 데이터를 버림
                .onBackpressureBuffer(10, BufferOverflowStrategy.DROP_LATEST)
                .subscribeOn(Schedulers.boundedElastic()); // I/O 작업을 위한 별도 스레드에서 실행
    }
}