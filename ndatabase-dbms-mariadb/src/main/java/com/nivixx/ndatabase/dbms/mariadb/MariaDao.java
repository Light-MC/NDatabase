package com.nivixx.ndatabase.dbms.mariadb;

import com.nivixx.ndatabase.api.exception.DatabaseCreationException;
import com.nivixx.ndatabase.api.model.NEntity;
import com.nivixx.ndatabase.dbms.jdbc.JdbcDao;
import com.nivixx.ndatabase.expressiontree.SingleNodePath;
import com.nivixx.ndatabase.platforms.coreplatform.logging.DBLogger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

public class MariaDao<K, V extends NEntity<K>> extends JdbcDao<K,V> {

    public MariaDao(String collectionName,
                    String schema,
                    Class<K> keyType,
                    Class<V> nEntityType,
                    V instantiatedNEntity,
                    HikariMariaConnectionPool hikariConnectionPool,
                    DBLogger dbLogger) {
        super(collectionName, schema, keyType, nEntityType, instantiatedNEntity, hikariConnectionPool,  dbLogger);
    }

    @Override
    public void createIndexes(List<SingleNodePath> singleNodePaths) throws DatabaseCreationException {
        try (Connection connection = pool.getConnection()) {
            for (SingleNodePath singleNodePath : singleNodePaths) {
                // Create column if not exist and index it
                denormalizeFieldIntoColumn(connection, singleNodePath);
            }
        } catch (SQLException e) {
            throw new DatabaseCreationException(
                    "Error during index creation by de-normalization for NEntity " + nEntityType.getCanonicalName(), e);
        }
    }

    protected void denormalizeFieldIntoColumn(Connection connection, SingleNodePath singleNodePath) throws SQLException {
        String columnName = singleNodePath.getFullPath("_");
        String fieldPath = singleNodePath.getFullPath(".");
        Class<?> fieldType = singleNodePath.getLastNodeType();

        // Using string concatenation or StringBuilder instead of MessageFormat
        // to avoid conflicts with SQL escape sequences
        String addColumnQuery = new StringBuilder()
                .append("ALTER TABLE ").append(collectionName)
                .append(" ADD COLUMN ").append(columnName)
                .append(" ").append(getColumnType(false, fieldType))
                .append(" GENERATED ALWAYS AS (JSON_VALUE(`")
                .append(DATA_IDENTIFIER).append("`, '$.").append(fieldPath).append("'))")
                .toString();

        try (PreparedStatement ps = connection.prepareStatement(addColumnQuery)) {
            ps.execute();
        } catch (SQLException e) {
            if (!e.getMessage().toLowerCase().contains("duplicate column name")) {
                throw e;
            }
        }

        String createIndexQuery = "CREATE INDEX IF NOT EXISTS " +
                columnName + "_index" +
                " ON " + collectionName +
                "(" + columnName + ")";

        try (PreparedStatement ps = connection.prepareStatement(createIndexQuery)) {
            ps.execute();
        }
    }
}
