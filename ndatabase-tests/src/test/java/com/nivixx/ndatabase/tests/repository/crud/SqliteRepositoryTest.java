package com.nivixx.ndatabase.tests.repository.crud;

import com.nivixx.ndatabase.api.NDatabase;
import com.nivixx.ndatabase.api.exception.NDatabaseException;
import com.nivixx.ndatabase.api.exception.NDatabaseLoadException;
import com.nivixx.ndatabase.api.repository.Repository;
import com.nivixx.ndatabase.core.config.DatabaseType;
import com.nivixx.ndatabase.core.config.NDatabaseConfig;
import com.nivixx.ndatabase.dbms.sqlite.SqliteConfig;
import com.nivixx.ndatabase.platforms.appplatform.AppNDatabaseConfig;
import com.nivixx.ndatabase.platforms.appplatform.AppPlatformLoader;
import com.nivixx.ndatabase.platforms.coreplatform.executor.SyncExecutor;
import com.nivixx.ndatabase.tests.TestLibraryManager;
import com.nivixx.ndatabase.tests.repository.entity.EmbeddedBukkitLocation;
import com.nivixx.ndatabase.tests.repository.entity.InvalidKeyTypeEntity;
import com.nivixx.ndatabase.tests.repository.entity.PlayerEntity;
import com.nivixx.ndatabase.tests.repository.entity.PlayerEntityNoIndex;
import net.byteflux.libby.LibraryManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class SqliteRepositoryTest extends AbstractRepositoryTest {
    public SqliteRepositoryTest() {
    }

    @Before
    public void initApp() throws NDatabaseLoadException, SQLException {
        String jdbcURL = "jdbc:h2:mem:test;MODE=MYSQL";
        ExecutorService mainThread = Executors.newFixedThreadPool(1);

        appPlatformLoader = new AppPlatformLoader() {
            @Override
            public SyncExecutor supplySyncExecutor() {
                return mainThread::execute;
            }

            @Override
            public NDatabaseConfig supplyNDatabaseConfig() {
                AppNDatabaseConfig bukkitNDatabaseConfig = new AppNDatabaseConfig();
                SqliteConfig sqliteConfig = new SqliteConfig();
                sqliteConfig.setFileFullPath("src/test/resources/testsqlite.sqlite");
                bukkitNDatabaseConfig.setDatabaseType(DatabaseType.SQLITE);
                bukkitNDatabaseConfig.setSqliteConfig(sqliteConfig);
                return bukkitNDatabaseConfig;
            }

            @Override
            public LibraryManager supplyLibraryManager() {
                File testDepenciesFolder = new File("src/test/resources/testDependencies");
                testDepenciesFolder.deleteOnExit();
                return new TestLibraryManager<>(Logger.getGlobal(), testDepenciesFolder.toPath(), this);
            }
        };
        File dbFile = new File("src/test/resources/testsqlite.sqlite");
        dbFile.deleteOnExit();
        appPlatformLoader.load();
        repository = NDatabase.api().getOrCreateRepository(PlayerEntity.class);
        repositoryNoIndex = NDatabase.api().getOrCreateRepository(PlayerEntityNoIndex.class);
        repositoryEmbeddedBukkitLocation = NDatabase.api().getOrCreateRepository(EmbeddedBukkitLocation.class);
    }

    @AfterClass
    public static void removeSqliteFile() {
    }

    @Test(expected = NDatabaseException.class)
    public void createDatabaseWithInvalidKeyType() {
        Repository<Thread, InvalidKeyTypeEntity> integerKeyDao =
                NDatabase.api().getOrCreateRepository(InvalidKeyTypeEntity.class);
        integerKeyDao.insert(new InvalidKeyTypeEntity(null));
    }
}
