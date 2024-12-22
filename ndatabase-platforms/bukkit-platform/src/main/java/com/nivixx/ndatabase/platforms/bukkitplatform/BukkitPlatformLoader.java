package com.nivixx.ndatabase.platforms.bukkitplatform;

import com.nivixx.ndatabase.core.PlatformLoader;
import com.nivixx.ndatabase.core.cache.CacheRepoConfig;
import com.nivixx.ndatabase.core.config.*;
import com.nivixx.ndatabase.dbms.mariadb.MariaDBConfig;
import com.nivixx.ndatabase.dbms.mongodb.MongoDBConfig;
import com.nivixx.ndatabase.dbms.mysql.MysqlConfig;
import com.nivixx.ndatabase.dbms.sqlite.SqliteConfig;
import com.nivixx.ndatabase.platforms.coreplatform.executor.SyncExecutor;
import com.nivixx.ndatabase.platforms.coreplatform.logging.DBLogger;
import net.byteflux.libby.BukkitLibraryManager;
import net.byteflux.libby.LibraryManager;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;

/**
 * For Spigot/Bukkit based servers
 */
public class BukkitPlatformLoader extends PlatformLoader {

    @Override
    public DBLogger supplyDbLogger(boolean isDebugMode) {
        return new BukkitDBLogger(isDebugMode);
    }

    @Override
    public SyncExecutor supplySyncExecutor() {
        return new BukkitSyncExecutor();
    }

    @Override
    public NDatabaseConfig supplyNDatabaseConfig() {
        NDatabasePlugin instance = NDatabasePlugin.getInstance();
        FileConfiguration config = instance.getConfig();

        ConfigurationSection mysql = config.getConfigurationSection("database.mysql");
        MysqlConfig mysqlConfig = new MysqlConfig();
        mysqlConfig.setHost(mysql.getString("host","localhost"));
        mysqlConfig.setClassName(mysql.getString("driver-class-name","com.mysql.jdbc.Driver"));
        mysqlConfig.setMinimumIdleConnection(mysql.getInt("minimum-idle-connection",1));
        mysqlConfig.setMaximumPoolSize(mysql.getInt("maximum-pool-size", 3));
        mysqlConfig.setPort(mysql.getInt("port", 3306));
        mysqlConfig.setDatabaseName(mysql.getString("database-name","ndatabase"));
        mysqlConfig.setUser(mysql.getString("user","user"));
        mysqlConfig.setPass(mysql.getString("pass","pass"));

        ConfigurationSection mariadb = config.getConfigurationSection("database.mariadb");
        MariaDBConfig mariadbConfig = new MariaDBConfig();
        mariadbConfig.setHost(mariadb.getString("host","localhost"));
        mariadbConfig.setClassName(mariadb.getString("driver-class-name","com.mysql.jdbc.Driver"));
        mariadbConfig.setMinimumIdleConnection(mariadb.getInt("minimum-idle-connection",1));
        mariadbConfig.setMaximumPoolSize(mariadb.getInt("maximum-pool-size", 3));
        mariadbConfig.setPort(mariadb.getInt("port", 3306));
        mariadbConfig.setDatabaseName(mariadb.getString("database-name","ndatabase"));
        mariadbConfig.setUser(mariadb.getString("user","user"));
        mariadbConfig.setPass(mariadb.getString("pass","pass"));

        ConfigurationSection sqlite = config.getConfigurationSection("database.sqlite");
        SqliteConfig sqliteConfig = new SqliteConfig();
        String fileName = sqlite.getString("file-name", "ndatabase.sqlite");
        sqliteConfig.setFileFullPath(NDatabasePlugin.getInstance().getDataFolder() + "/" + fileName);

        ConfigurationSection mongoDB = config.getConfigurationSection("database.mongodb");
        MongoDBConfig mongoDBConfig = new MongoDBConfig();
        String mongoDBHost = mongoDB.getString("host", "localhost");
        String mongoDBDatabase = mongoDB.getString("database", "ndatabase");
        int mongoDBPort = mongoDB.getInt("port", 27017);
        String mongoDBUser = mongoDB.getString("user", "");
        String mongoDBPass = mongoDB.getString("pass", "");
        mongoDBConfig.setHost(mongoDBHost);
        mongoDBConfig.setPort(mongoDBPort);
        mongoDBConfig.setDatabase(mongoDBDatabase);
        mongoDBConfig.setUser(mongoDBUser);
        mongoDBConfig.setPass(mongoDBPass);

        ConfigurationSection cache = config.getConfigurationSection("cache");
        CacheRepoConfig cacheRepoConfig = new CacheRepoConfig();
        long expireAfterAccessMinutes = cache.getLong("expire-after-access-minutes", 30);
        long maximumSize = cache.getLong("maximum-size", 10_000);
        int getTimeoutSeconds = cache.getInt("get-timeout-seconds", 5);
        boolean enableSoftValues = cache.getBoolean("enable-soft-values", false);
        boolean enableRefreshAfterWrite = cache.getBoolean("enable-refresh-after-write", false);
        long refreshAfterWriteMinutes = cache.getLong("refresh-after-write-minutes", 30);
        boolean enableStats = cache.getBoolean("enable-stats", false);

        cacheRepoConfig.setExpireAfterAccessMinutes(expireAfterAccessMinutes);
        cacheRepoConfig.setMaximumSize(maximumSize);
        cacheRepoConfig.setGetTimeoutSeconds(getTimeoutSeconds);
        cacheRepoConfig.setEnableSoftValues(enableSoftValues);
        cacheRepoConfig.setEnableRefreshAfterWrite(enableRefreshAfterWrite);
        cacheRepoConfig.setRefreshAfterWriteMinutes(refreshAfterWriteMinutes);
        cacheRepoConfig.setEnableStats(enableStats);

        boolean debug = config.getBoolean("debug-mode", false);

        BukkitNDatabaseConfig bukkitNDatabaseConfig = new BukkitNDatabaseConfig();
        bukkitNDatabaseConfig.setDebugMode(debug);
        bukkitNDatabaseConfig.setIdleThreadPoolSize(config.getInt("idle-thread-pool-size",3));
        bukkitNDatabaseConfig.setDatabaseType(DatabaseType.valueOf(config.getString("database-type")));
        bukkitNDatabaseConfig.setMysqlConfig(mysqlConfig);
        bukkitNDatabaseConfig.setMariaDBConfig(mariadbConfig);
        bukkitNDatabaseConfig.setSqliteConfig(sqliteConfig);
        bukkitNDatabaseConfig.setMongoDBConfig(mongoDBConfig);
        bukkitNDatabaseConfig.setCacheRepoConfig(cacheRepoConfig);

        return bukkitNDatabaseConfig;
    }

    @Override
    public LibraryManager supplyLibraryManager() {
        return new BukkitLibraryManager(NDatabasePlugin.getInstance(), "ndatabase-dependencies");
    }
}
