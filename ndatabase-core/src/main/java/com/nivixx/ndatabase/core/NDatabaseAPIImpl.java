package com.nivixx.ndatabase.core;

import com.google.inject.Inject;
import com.nivixx.ndatabase.api.NDatabaseAPI;
import com.nivixx.ndatabase.api.exception.NDatabaseException;
import com.nivixx.ndatabase.api.model.NEntity;
import com.nivixx.ndatabase.api.repository.Repository;

@SuppressWarnings({"unchecked","rawtypes"})
public class NDatabaseAPIImpl implements NDatabaseAPI {

    private final RepositoryManager bloodyDaoManager;

    @Inject
    public NDatabaseAPIImpl(RepositoryManager bloodyDaoManager) {
        this.bloodyDaoManager = bloodyDaoManager;
    }

    public <K,V extends NEntity<K>> Repository<K,V> getOrCreateRepository(Class<V> entityType) throws NDatabaseException {
        return bloodyDaoManager.getOrCreateDao(entityType);
    }
}
