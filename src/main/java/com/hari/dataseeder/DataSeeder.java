package com.hari.dataseeder;

import com.hari.repository.repoUtil.RepoUtil;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;

public class DataSeeder {

    private static final int MAX_RETRIES    = 20;
    private static final long RETRY_DELAY_MS = 3_000;

    public static void migrate() throws Exception {
        DataSource ds = RepoUtil.getDataSource();

        Flyway flyway = Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();
        flyway.migrate();
    }
}


