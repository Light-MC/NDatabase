package com.nivixx.ndatabase.dbms.mariadb;

import com.nivixx.ndatabase.dbms.jdbc.JdbcConnectionPool;
import com.nivixx.ndatabase.platforms.coreplatform.logging.DBLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.byteflux.libby.Library;
import net.byteflux.libby.LibraryManager;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariMariaConnectionPool implements JdbcConnectionPool {

    private final MariaDBConfig mariaDBConfig;
    private final LibraryManager libraryManager;
    private final DBLogger dbLogger;

    private HikariDataSource dataSource;
    private ClassLoader driverClassLoader;

    public HikariMariaConnectionPool(MariaDBConfig mariaDBConfig, DBLogger dbLogger, LibraryManager libraryManager) {
        this.mariaDBConfig = mariaDBConfig;
        this.dbLogger = dbLogger;
        this.libraryManager = libraryManager;
    }

    @Override
    public void connect() throws Exception {
        installMariaDBDriver();

        Thread thread = Thread.currentThread();
        ClassLoader previousClassLoader = thread.getContextClassLoader();
        thread.setContextClassLoader(driverClassLoader);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(mariaDBConfig.getHost());
        config.setDriverClassName(mariaDBConfig.getClassName());
        config.setUsername(mariaDBConfig.getUser());
        config.setPassword(mariaDBConfig.getPass());
        config.setMinimumIdle(mariaDBConfig.getMinimumIdleConnection());
        config.setMaximumPoolSize(20);
        config.setConnectionTimeout(4000);

        dataSource = new HikariDataSource(config);

        thread.setContextClassLoader(previousClassLoader);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private void installMariaDBDriver() {
        Library lib = Library.builder()
                .groupId("org{}mariadb{}jdbc")
                .artifactId("mariadb-java-client")
                .version("2.7.1")
                .isolatedLoad(true)
                .id("mariadb-java-client")
                .build();
        dbLogger.logInfo("Loading MariaDB driver...");
        libraryManager.loadLibrary(lib);
        driverClassLoader = libraryManager.getIsolatedClassLoaderOf("mariadb-java-client");
        dbLogger.logInfo("MariaDB driver loaded successfully.");
    }

    public void closePool() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

}