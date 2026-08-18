package com.hari.config;

import com.hari.dataseeder.DataSeeder;
import jakarta.servlet.FilterRegistration;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;

public class AppInitializer extends AbstractAnnotationConfigDispatcherServletInitializer {
    @Override
    public void onStartup(ServletContext servletContext) throws ServletException {
        try {
            DataSeeder.migrate();
        } catch (Exception e) {
            throw new ServletException("Flyway migration failed — cannot start application", e);
        }
        super.onStartup(servletContext);
    }

    @Override
    protected Class<?>[] getRootConfigClasses() {
        return new Class[0];
    }

    @Override
    protected Class<?>[] getServletConfigClasses() {
        return new Class[]{WebConfig.class};
    }

    @Override
    protected String[] getServletMappings() {
        return new String[]{"/"};
    }
}
