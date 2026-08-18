package com.hari.config;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.CacheControl;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableWebMvc
@ComponentScan(basePackages = {"com.hari", "exception"})
public class WebConfig {
}
