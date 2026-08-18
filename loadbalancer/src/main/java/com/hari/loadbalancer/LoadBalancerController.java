package com.hari.loadbalancer;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
public class LoadBalancerController {

    private final String backendHost;
    private final int backendPort;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final RestTemplate restTemplate = new RestTemplate();

    public LoadBalancerController(@Value("${backend.host}") String backendHost,
                                  @Value("${backend.port}") int backendPort) {
        this.backendHost = backendHost;
        this.backendPort = backendPort;
    }

    @RequestMapping("/**")
    public ResponseEntity<String> forward(HttpServletRequest request, @RequestBody(required = false) String body) throws Exception {
        InetAddress[] addresses = InetAddress.getAllByName(backendHost);
        List<String> backends = Arrays.stream(addresses)
                .map(a -> "http://" + a.getHostAddress() + ":" + backendPort).toList();
        
        int index = Math.abs(counter.getAndIncrement() % backends.size());
        String backend = backends.get(index);

        String url = backend + request.getRequestURI();
        HttpMethod method = HttpMethod.valueOf(request.getMethod());

        System.out.println("➡️  [" + index + 1 + "/" + backends.size() + "] " + method + " → " + url);

        HttpHeaders headers = new org.springframework.http.HttpHeaders();
        if (request.getContentType() != null) {
            headers.set("Content-Type", request.getContentType());
        }

        HttpEntity<String> entity = new HttpEntity<>(body, headers);
        ResponseEntity<String> response = restTemplate.exchange(url, method, entity, String.class);

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("X-Upstream-Server", backend);
        responseHeaders.set("X-Round-Robin-Index", String.valueOf(index + 1));

        return new ResponseEntity<>(response.getBody(), responseHeaders, response.getStatusCode());
    }
}
