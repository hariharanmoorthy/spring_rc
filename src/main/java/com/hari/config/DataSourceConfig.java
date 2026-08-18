package com.hari.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class DataSourceConfig {

    public static DataSource build() {
        String host     = getEnv("DB_HOST",     "localhost");
        String port     = getEnv("DB_PORT",     "5432");
        String dbName   = getEnv("DB_NAME",     "spring_rc_db");
        String user     = getEnv("DB_USER",     "spring_rc_user");
        String password = getEnv("DB_PASSWORD", "changeme");

        String jdbcUrl = String.format(
            "jdbc:postgresql://%s:%s/%s", host, port, dbName);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setDriverClassName("org.postgresql.Driver");

        // Pool tuning (safe defaults for container)
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30_000);
        config.setIdleTimeout(600_000);
        config.setMaxLifetime(1_800_000);
        config.setConnectionTestQuery("SELECT 1");
        config.setPoolName("spring-rc-pool");

        return new HikariDataSource(config);
    }

    private static String getEnv(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
