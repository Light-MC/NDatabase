package com.nivixx.ndatabase.core;

import com.nivixx.ndatabase.api.annotation.UseCache;
import com.nivixx.ndatabase.api.exception.DatabaseCreationException;
import com.nivixx.ndatabase.api.exception.NDatabaseException;
import com.nivixx.ndatabase.api.model.NEntity;
import com.nivixx.ndatabase.core.cache.CacheRepoConfig;
import com.nivixx.ndatabase.core.cache.CachedRepositoryImpl;
import com.nivixx.ndatabase.core.config.NDatabaseConfig;
import com.nivixx.ndatabase.dbms.api.Dao;
import com.nivixx.ndatabase.expressiontree.SingleNodePath;
import com.nivixx.ndatabase.api.repository.Repository;
import com.nivixx.ndatabase.core.promise.AsyncThreadPool;
import com.nivixx.ndatabase.core.reflection.NReflectionUtil;
import com.nivixx.ndatabase.platforms.coreplatform.executor.SyncExecutor;
import com.nivixx.ndatabase.platforms.coreplatform.logging.DBLogger;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RepositoryManager<K,V extends NEntity<K>> {

    private final Map<Class<V>, Repository<K,V>> repositoryCache;
    private final Map<String, Set<Class<V>>> pluginEntityMap;

    private final DatabaseTypeResolver databaseTypeResolver;

    public RepositoryManager() {
        this.repositoryCache = new ConcurrentHashMap<>();
        this.pluginEntityMap = new ConcurrentHashMap<>();
        this.databaseTypeResolver = new DatabaseTypeResolver();
    }

    public Repository<K,V> getOrCreateRepository(Class<V> entityType) throws NDatabaseException {
        if(repositoryCache.containsKey(entityType)) {
            return repositoryCache.get(entityType);
        }

        // Create an instance of this type and resolve the key type
        V nEntity = createEntityInstance(entityType);
        Class<K> keyType = resolveKeyFromEntity(nEntity);

        // Find configured database type (MYSQL, MongoDB, ...)
        Dao<K,V> dao = databaseTypeResolver.getDaoForConfiguredDatabase(nEntity, keyType, entityType);

        // Init stuff for target DB if needed
        dao.init();

        // Test database connection
        dao.validateConnection();

        // Create the database/schema structure if doesn't exist
        dao.createDatabaseIfNotExist(keyType);

        // Index the key-value store
        List<SingleNodePath> indexSingleNodePathList = new ArrayList<>();
        try {
            NReflectionUtil.resolveIndexedFieldsFromEntity(indexSingleNodePathList, new SingleNodePath(), nEntity);
        } catch (Exception e) {
            throw new DatabaseCreationException("Failed to resolve nEntity index paths ", e);
        }
        dao.createIndexes(indexSingleNodePathList);

        CacheRepoConfig cacheRepoConfig = Injector.resolveInstance(NDatabaseConfig.class).getCacheRepoConfig();

        // Init repository
        DBLogger dbLogger = Injector.resolveInstance(DBLogger.class);
        SyncExecutor syncExecutor = Injector.resolveInstance(SyncExecutor.class);
        AsyncThreadPool asyncThreadPool = Injector.resolveInstance(AsyncThreadPool.class);
        Repository<K,V> repository = useCachedRepo(entityType) ?
                new CachedRepositoryImpl<>(dao, entityType, syncExecutor, asyncThreadPool, dbLogger, cacheRepoConfig) :
                new RepositoryImpl<>(dao, entityType, syncExecutor, asyncThreadPool, dbLogger);
        repositoryCache.put(entityType, repository);

        ClassLoader classLoader = entityType.getClassLoader();
        if (classLoader != null && classLoader.toString().contains("PluginClassLoader")) {
            String pluginName = extractPluginName(classLoader.toString());
            pluginEntityMap.computeIfAbsent(pluginName, k -> ConcurrentHashMap.newKeySet())
                    .add(entityType);
        }

        return repository;
    }

    public void shutdownCache() throws NDatabaseException {
        String callingPlugin = findCallingPlugin();
        if (callingPlugin == null) {
            throw new NDatabaseException("Could not find the calling plugin");
        }

        Set<Class<V>> pluginEntities = pluginEntityMap.get(callingPlugin);
        if (pluginEntities != null) {
            for (Class<V> entityClass : pluginEntities) {
                Repository<K,V> repository = repositoryCache.get(entityClass);
                if (repository instanceof CachedRepositoryImpl) {
                    ((CachedRepositoryImpl<K,V>) repository).shutdown();
                }
            }
        }

        pluginEntityMap.remove(callingPlugin);
    }

    private String findCallingPlugin() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (int i = 2; i < stackTrace.length; i++) {
            try {
                Class<?> callingClass = Class.forName(stackTrace[i].getClassName());
                ClassLoader classLoader = callingClass.getClassLoader();
                if (classLoader != null &&
                        classLoader.toString().contains("PluginClassLoader") &&
                        !classLoader.toString().contains("NDatabase")) {
                    String pluginName = extractPluginName(classLoader.toString());
                    if (pluginName != null) {
                        return pluginName;
                    }
                }
            } catch (ClassNotFoundException ignored) {}
        }

        return null;
    }

    private String extractPluginName(String classLoaderString) {
        //PluginClassLoader{plugin=ndbTest v1.0-SNAPSHOT, pluginEnabled=false}
        // plugin name would be ndbTest v1.0-SNAPSHOT
        int start = classLoaderString.indexOf("plugin=") + 7;
        int end = classLoaderString.indexOf(", pluginEnabled=", start);
        if (start > 0 && end > start) {
            return classLoaderString.substring(start, end);
        }
        return null;
    }

    private V createEntityInstance(Class<V> entityClass) throws DatabaseCreationException {
        try {
            return entityClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new DatabaseCreationException(
                    String.format("could not instantiate NEntity class '%s'." +
                                    " /!\\ Don't forget you have to create a default empty constructor for your entity object",
                            entityClass.getCanonicalName()), e);
        }
    }

    private boolean useCachedRepo(Class<V> entityType) {
        return entityType.isAnnotationPresent(UseCache.class);
    }

    @SuppressWarnings("unchecked")
    private Class<K> resolveKeyFromEntity(V nEntity) {
        try {
            ParameterizedType genericSuperclass = (ParameterizedType) nEntity.getClass().getGenericSuperclass();
            Type actualTypeArgument = genericSuperclass.getActualTypeArguments()[0];
            return (Class<K>) actualTypeArgument;
        } catch (Exception e) {
            throw new DatabaseCreationException(
                    String.format("could not resolve the key type for NEntity class '%s'." +
                                    " Did you properly used a supported key type ?",
                            nEntity.getClass().getCanonicalName()), e);
        }
    }

}
