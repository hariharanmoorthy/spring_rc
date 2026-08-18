package com.hari.repository.repoUtil;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * Centralised JDBC utility backed by a HikariCP connection pool.
 * All credentials are sourced from environment variables; never hardcoded.
 * <p>
 * Thread-safety: all public methods are stateless and share the single
 * {@code HikariDataSource} which is itself thread-safe.
 */
public final class RepoUtil {

    // ── Connection pool ──────────────────────────────────────────────────────

    private static final HikariDataSource DATA_SOURCE;

    static {
        HikariConfig cfg = new HikariConfig();

        // Support BOTH connection styles:
        // Style 1 (compose.yaml): DB_HOST + DB_PORT + DB_NAME individually
        // Style 2 (legacy):       DB_URL as full JDBC URL
        String jdbcUrl = env("DB_URL", null);
        if (jdbcUrl == null) {
            // Build URL from individual parts (used in container / compose)
            String host = env("DB_HOST", "localhost");
            String port = env("DB_PORT", "5432");
            String name = env("DB_NAME", "springlearn");
            jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + name;
        }

        cfg.setJdbcUrl(jdbcUrl);
        // DB_USERNAME falls back to DB_USER for compose.yaml compatibility
        cfg.setUsername(env("DB_USERNAME", env("DB_USER", "hari")));
        cfg.setPassword(env("DB_PASSWORD", "hari"));
        cfg.setDriverClassName("org.postgresql.Driver");

        // Pool tuning — sensible defaults for a small service
        cfg.setMaximumPoolSize(10);
        cfg.setMinimumIdle(2);
        cfg.setConnectionTimeout(30_000);
        cfg.setIdleTimeout(600_000);
        cfg.setMaxLifetime(1_800_000);
        cfg.setConnectionTestQuery("SELECT 1");
        cfg.setPoolName("RC-Pool");

        DATA_SOURCE = new HikariDataSource(cfg);
    }

    private RepoUtil() { /* utility class */ }

    public static DataSource getDataSource() { return DATA_SOURCE; }

    public static Map<String, Object> executeQuery(String sql, Object... params) throws SQLException {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapRow(rs);
            }
        }
        return Collections.emptyMap();
    }

    /**
     * Fetch all rows matching a SELECT query.
     */
    public static List<Map<String, Object>> fetchQuery(String sql, Object... params) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) results.add(mapRow(rs));
            }
        }
        return results;
    }

    /**
     * Fetch at most one row, returned as an {@code Optional}.
     */
    public static Optional<Map<String, Object>> fetchOne(String sql, Object... params) throws SQLException {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return Optional.of(mapRow(rs));
            }
        }
        return Optional.empty();
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private static void bind(PreparedStatement ps, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }

    private static Map<String, Object> mapRow(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        Map<String, Object> row = new LinkedHashMap<>(count);
        for (int i = 1; i <= count; i++) {
            row.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        return row;
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : fallback;
    }
}
