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
                .evictionListener((K key, V value, RemovalCause cause) -> {
                    if (value != null && (cause.wasEvicted() || cause == RemovalCause.REPLACED)) {
                        dao.upsert(value);
                    }
                })
                .buildAsync((key, executor) -> CompletableFuture.supplyAsync(
                        () -> dao.get(key, classz),
                        asyncThreadPool.getExecutor()
                ));

    }

    @Override
    public V get(K key) throws NDatabaseException {
        try {
            return cache.get(key).get(cacheConfig.getGetTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new NDatabaseException("Failed to get value from cache", e);
        }
    }

    @Override
    public Promise.AsyncResult<V> getAsync(K key) {
        return new PromiseResultPipeline<>(cache.get(key), syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public Promise.AsyncResult<Optional<V>> findOneAsync(Predicate<V> predicate) {
        CompletableFuture<Optional<V>> future = CompletableFuture.supplyAsync(() ->
                        cache.synchronous()
                                .asMap()
                                .values()
                                .stream()
                                .filter(predicate)
                                .findFirst(),
                asyncThreadPool.getExecutor()
        );
        return new PromiseResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    @Override
    public Promise.AsyncResult<Optional<V>> streamAndFindOneAsync(Predicate<V> predicate) {
        return findOneAsync(predicate);
    }

    @Override
    public Promise.AsyncResult<List<V>> findAsync(Predicate<V> predicate) {
        CompletableFuture<List<V>> future = CompletableFuture.supplyAsync(() ->
                        cache.synchronous()
                                .asMap()
                                .values()
                                .stream()
                                .filter(predicate)
                                .collect(Collectors.toList()),
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
    }

    @Override
    public Promise.AsyncEmptyResult deleteAsync(K key)  {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> cache.asMap().remove(key));
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
    }

    @Override
    public Promise.AsyncEmptyResult deleteAllAsync() throws NDatabaseException {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> cache.asMap().clear());
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

    public Promise.AsyncResult<Map<K, V>> getAllAsync(Collection<K> keys) {
        CompletableFuture<Map<K, V>> future = cache.getAll(keys);
        return new PromiseResultPipeline<>(future, syncExecutor, asyncThreadPool, dbLogger);
    }

    public void flushCache() {
        cache.synchronous().invalidateAll();
    }
}
