package com.hari.loadbalancer;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.juli.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
public class LoadBalancerController {

    private static final Logger log = LoggerFactory.getLogger(LoadBalancerController.class);

    private final String backendHost;
    private final int backendPort;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final RestTemplate restTemplate;
    private final RateLimiterService rateLimiterService;

    public LoadBalancerController(@Value("${backend.host}") String backendHost,
                                   @Value("${backend.port}") int backendPort,
                                   @Value("${gateway.connect-timeout-ms:2000}") int connectTimeoutMs,
                                   @Value("${gateway.read-timeout-ms:5000}") int readTimeoutMs,
                                   RateLimiterService rateLimiterService) {
        this.backendHost = backendHost;
        this.backendPort = backendPort;
        this.rateLimiterService = rateLimiterService;
        this.restTemplate = new RestTemplate(clientFactory(connectTimeoutMs, readTimeoutMs));
    }

    private static ClientHttpRequestFactory clientFactory(int connectTimeoutMs, int readTimeoutMs) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(readTimeoutMs);
        return factory;
    }

    @RequestMapping("/**")
    public ResponseEntity<String> forward(HttpServletRequest request, @RequestBody(required = false) String body) {
        String clientKey = resolveClientKey(request);

        RateLimiterService.RateLimitDecision decision = rateLimiterService.checkLimit(clientKey);
        HttpHeaders rateLimitHeaders = buildRateLimitHeaders(decision);
        System.out.println("checking value"+ decision.remainingTokens() + " allowed"+ decision.allowed());


        if (!decision.allowed()) {
            rateLimitHeaders.set("Retry-After", "1");
            return new ResponseEntity<>(
                    "Rate limit exceeded. Please slow down and try again shortly.",
                    rateLimitHeaders, HttpStatus.TOO_MANY_REQUESTS);
        }

        List<String> backends;
        try {
            backends = resolveBackends();
        } catch (UnknownHostException e) {
            log.error("Unable to resolve backend host '{}': {}", backendHost, e.getMessage());
            return new ResponseEntity<>("backend host unavailable",
                    rateLimitHeaders, HttpStatus.BAD_GATEWAY);
        }

        if (backends.isEmpty()) {
            log.error("No backend instances resolved for host '{}'", backendHost);
            return new ResponseEntity<>("Service Unavailable: no backend instances",
                    rateLimitHeaders, HttpStatus.SERVICE_UNAVAILABLE);
        }

        int index = Math.abs(counter.getAndIncrement() % backends.size());
        String backend = backends.get(index);
        String url = backend + request.getRequestURI() +
                (request.getQueryString() != null ? "?" + request.getQueryString() : "");

        try {
            HttpMethod method = HttpMethod.valueOf(request.getMethod());

            HttpHeaders headers = new HttpHeaders();
            if (request.getContentType() != null) {
                headers.set("Content-Type", request.getContentType());
            }

            HttpEntity<String> entity = new HttpEntity<>(body,headers);
            ResponseEntity<String> response = restTemplate.exchange(url, method, entity, String.class);

            rateLimitHeaders.set("X-Upstream-Server", backend);
            rateLimitHeaders.set("X-Round-Robin-Index", String.valueOf(index + 1));

            return new ResponseEntity<>(response.getBody(), rateLimitHeaders, response.getStatusCode());
        } catch (HttpStatusCodeException e) {
            rateLimitHeaders.set("X-Upstream-Server", backend);
            return new ResponseEntity<>(e.getResponseBodyAsString(), rateLimitHeaders, e.getStatusCode());
        } catch (ResourceAccessException e) {
            log.warn("Upstream '{}' timed out or was unreachable: {}", backend, e.getMessage());
            return new ResponseEntity<>("Gateway Timeout: upstream did not respond in time",
                    rateLimitHeaders, HttpStatus.GATEWAY_TIMEOUT);
        } catch (RestClientException e) {
            log.error("Unexpected error forwarding request to '{}': {}", backend, e.getMessage(), e);
            return new ResponseEntity<>("Bad Gateway: error communicating with upstream",
                    rateLimitHeaders, HttpStatus.BAD_GATEWAY);
        }
    }

    private List<String> resolveBackends() throws UnknownHostException {
        InetAddress[] addresses = InetAddress.getAllByName(backendHost);
        return Arrays.stream(addresses)
                .map(a -> "http://" + a.getHostAddress() + ":" + backendPort)
                .toList();
    }

    private HttpHeaders buildRateLimitHeaders(RateLimiterService.RateLimitDecision decision) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-RateLimit-Limit", String.valueOf(decision.capacity()));
        headers.set("X-RateLimit-Remaining", String.valueOf(decision.remainingTokens()));
        return headers;
    }

    private String resolveClientKey(HttpServletRequest request) {
        String forwardedFor = request.getHeader("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isBlank()) {
            return forwardedFor.split(",")[0].trim();
        }
        System.out.println("address +" + request.getRemoteAddr());
        return request.getRemoteAddr();
    }
}
