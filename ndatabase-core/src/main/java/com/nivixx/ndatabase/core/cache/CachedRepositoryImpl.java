package com.nivixx.ndatabase.core.cache;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.nivixx.ndatabase.api.Promise;
import com.nivixx.ndatabase.api.exception.NDatabaseException;
import com.nivixx.ndatabase.api.model.NEntity;
import com.nivixx.ndatabase.api.query.NQuery;
import com.nivixx.ndatabase.api.repository.Repository;
import com.nivixx.ndatabase.core.promise.AsyncThreadPool;
import com.nivixx.ndatabase.core.promise.pipeline.PromiseEmptyResultPipeline;
import com.nivixx.ndatabase.core.promise.pipeline.PromiseResultPipeline;
import com.nivixx.ndatabase.dbms.api.Dao;
import com.nivixx.ndatabase.platforms.coreplatform.executor.SyncExecutor;
import com.nivixx.ndatabase.platforms.coreplatform.logging.DBLogger;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CachedRepositoryImpl<K, V extends NEntity<K>> implements Repository<K, V> {

    private final AsyncLoadingCache<K, V> cache;

    private final SyncExecutor syncExecutor;
    private final AsyncThreadPool asyncThreadPool;
    private final DBLogger dbLogger;
    private final CacheRepoConfig cacheConfig;
    private final Dao<K, V> dao;

    public CachedRepositoryImpl(Dao<K, V> dao,
                          Class<V> classz,
                          SyncExecutor syncExecutor,
                          AsyncThreadPool asyncThreadPool,
                          DBLogger dbLogger,
                          CacheRepoConfig cacheConfig) {
        this.syncExecutor = syncExecutor;
        this.asyncThreadPool = asyncThreadPool;
        this.dbLogger = dbLogger;
        this.cacheConfig = cacheConfig;
        this.dao = dao;

        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .expireAfterAccess(cacheConfig.getExpireAfterAccessMinutes(), TimeUnit.MINUTES)
                .maximumSize(cacheConfig.getMaximumSize())
                .executor(asyncThreadPool.getExecutor());

        if (cacheConfig.isEnableStats()) {
            builder.recordStats();
        }

        if (cacheConfig.isEnableSoftValues()) {
            builder.softValues();
        }

        if (cacheConfig.isEnableRefreshAfterWrite()) {
            builder.refreshAfterWrite(cacheConfig.getRefreshAfterWriteMinutes(), TimeUnit.MINUTES);
        }

        this.cache = builder
                .removalListener((K key, V value, RemovalCause cause) -> {
                    dbLogger.logInfo("Cached entity: " + key + " was evicted");
                    try {
                        dao.upsert(value);
                    } catch (NDatabaseException e) {
                        dbLogger.logError(e, "Error flushing entity from cache");
                        return;
                    }

                    dbLogger.logInfo("Upserted entity with key: " + key);
                })
                .buildAsync((key, executor) -> CompletableFuture.supplyAsync(
                        () -> dao.get(key, classz),
                        asyncThreadPool.getExecutor()
                ));
    }

    @Override
    public V get(K key) throws NDatabaseException {
        try {
            CompletableFuture<V> future = cache.get(key);

            boolean hitCache = cache.synchronous().asMap().containsKey(key);
            if (hitCache) {
                dbLogger.logInfo("Cache hit for key: " + key);
            } else {
                dbLogger.logInfo("Cache miss for key: " + key);
            }

            return future.get(cacheConfig.getGetTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new NDatabaseException("Failed to get value from cache", e);
        }
    }

    @Override
    public Promise.AsyncResult<V> getAsync(K key) {
        boolean hitCache = cache.synchronous().asMap().containsKey(key);
        if (hitCache) {
            dbLogger.logInfo("Cache hit for key: " + key);
        } else {
            dbLogger.logInfo("Cache miss for key: " + key);
        }

        return new PromiseResultPipeline<>(cache.get(key), syncExecutor, asyncThreadPool, dbLogger);    }

    @Override
    public Promise.AsyncResult<Optional<V>> findOneAsync(Predicate<V> predicate) {
        CompletableFuture<Optional<V>> future = CompletableFuture.supplyAsync(() -> {
            Optional<V> result = cache.synchronous()
                    .asMap()
                    .values()
                    .stream()
                    .filter(predicate)
                    .findFirst();
            if (result.isPresent()) {
                dbLogger.logInfo("Cache hit for value matching predicate: " + result.get());
            } else {
                dbLogger.logInfo("Cache miss for value matching predicate (none found).");
            }
            return result;
        }, asyncThreadPool.getExecutor());
        return new PromiseResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public Promise.AsyncResult<Optional<V>> streamAndFindOneAsync(Predicate<V> predicate) {
        return findOneAsync(predicate);
    }

    @Override
    public Promise.AsyncResult<List<V>> findAsync(Predicate<V> predicate) {
        CompletableFuture<List<V>> future = CompletableFuture.supplyAsync(() -> {
                    List<V> result = cache.synchronous()
                            .asMap()
                            .values()
                            .stream()
                            .filter(predicate)
                            .collect(Collectors.toList());

                    if (!result.isEmpty()) {
                        dbLogger.logInfo("Cache hit for values matching predicate: " + result.size() + " items found.");
                    } else {
                        dbLogger.logInfo("Cache miss for values matching predicate (none found).");
                    }
                    return result;
                },
                asyncThreadPool.getExecutor()
        );
        return new PromiseResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public Promise.AsyncResult<List<V>> streamAndFindAsync(Predicate<V> predicate) {
        return findAsync(predicate);
    }

    @Override
    public void insert(V value) throws NDatabaseException {
        if (value == null) {
            throw new NDatabaseException("Cannot insert null value");
        }
        cache.synchronous().put(value.getKey(), value);
    }

    @Override
    public Promise.AsyncEmptyResult insertAsync(V value) {
        if (value == null) {
            return new PromiseEmptyResultPipeline<>(
                    CompletableFuture.supplyAsync(() -> { throw new NDatabaseException("Cannot insert null value"); }),
                    syncExecutor, asyncThreadPool, dbLogger
            );
        }
        CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
                        cache.put(value.getKey(), CompletableFuture.completedFuture(value)),
                asyncThreadPool.getExecutor()
        );
        return new PromiseEmptyResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public void upsert(V value) throws NDatabaseException {
        insert(value);
    }

    @Override
    public Promise.AsyncEmptyResult upsertAsync(V value) {
        return insertAsync(value);
    }

    @Override
    public void update(V value) throws NDatabaseException {
        insert(value);
    }

    @Override
    public Promise.AsyncEmptyResult updateAsync(V value) {
        return insertAsync(value);
    }

    @Override
    public void delete(K key) throws NDatabaseException {
        cache.synchronous().asMap().remove(key);
        dao.delete(key);
    }

    @Override
    public Promise.AsyncEmptyResult deleteAsync(K key)  {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            cache.asMap().remove(key);
            dao.delete(key);
        });
        return new PromiseEmptyResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public void delete(V value) throws NDatabaseException {
        delete(value.getKey());
    }

    @Override
    public Promise.AsyncEmptyResult deleteAsync(V value)  {
        return deleteAsync(value.getKey());
    }

    @Override
    public void deleteAll() throws NDatabaseException {
        cache.synchronous().asMap().clear();
        dao.deleteAll();
    }

    @Override
    public Promise.AsyncEmptyResult deleteAllAsync() throws NDatabaseException {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            cache.asMap().clear();
            dao.deleteAll();
        });
        return new PromiseEmptyResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public Stream<V> streamAllValues() throws NDatabaseException {
        return cache.synchronous().asMap().values().stream();
    }

    @Override
    public Promise.AsyncResult<Stream<V>> streamAllValuesAsync() throws NDatabaseException {
        CompletableFuture<Stream<V>> future = CompletableFuture.supplyAsync(() ->
                        cache.synchronous()
                                .asMap()
                                .values()
                                .stream(),
                asyncThreadPool.getExecutor()
        );
        return new PromiseResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public Promise.AsyncResult<List<V>> computeTopAsync(int topMax, Comparator<V> comparator) {
        CompletableFuture<List<V>> future = CompletableFuture.supplyAsync(() ->
                        cache.synchronous()
                                .asMap()
                                .values()
                                .stream()
                                .sorted(comparator)
                                .limit(topMax)
                                .collect(Collectors.toList()),
                asyncThreadPool.getExecutor()
        );
        return new PromiseResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public Optional<V> findOne(NQuery.Predicate predicate) {
        throw new UnsupportedOperationException("The provided predicate is SQL-based and cannot be applied to in-memory objects.");
    }

    @Override
    public Promise.AsyncResult<Optional<V>> findOneAsync(NQuery.Predicate predicate) {
        throw new UnsupportedOperationException("The provided predicate is SQL-based and cannot be applied to in-memory objects.");
    }

    @Override
    public List<V> find(NQuery.Predicate predicate) {
        throw new UnsupportedOperationException("The provided predicate is SQL-based and cannot be applied to in-memory objects.");
    }

    @Override
    public Promise.AsyncResult<List<V>> findAsync(NQuery.Predicate predicate) {
        throw new UnsupportedOperationException("The provided predicate is SQL-based and cannot be applied to in-memory objects.");
    }

    public void flushCache() {
        dbLogger.logInfo("Flushing cache");
        cache.synchronous().invalidateAll();
    }

    public void shutdown() throws NDatabaseException {
        Map<K, V> cachedValues = cache.synchronous().asMap();
        dbLogger.logInfo("Shutting down cache with values: " + cachedValues);
        for (Map.Entry<K, V> entry : cachedValues.entrySet()) {
            try {
                dbLogger.logInfo("Flushing cache entry: " + entry.getKey());
                dao.upsert(entry.getValue());
            } catch (NDatabaseException e) {
                dbLogger.logError(e, "Error upserting entity during shutdown: " + entry.getKey());
            }
        }
        // no invalidation, as the cache will be wiped on the shutdown (all in memory)
        // we cannot trigger the removal listener because the jar has been unloaded by bukkit
    }
}
