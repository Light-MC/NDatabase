package com.nivixx.ndatabase.core.dao.mariadb;

import com.nivixx.ndatabase.api.exception.DatabaseCreationException;
import com.nivixx.ndatabase.api.model.NEntity;
import com.nivixx.ndatabase.core.expressiontree.SingleNodePath;
import com.nivixx.ndatabase.core.dao.jdbc.JdbcDao;
import com.nivixx.ndatabase.platforms.coreplatform.logging.DBLogger;

import java.sql.Connection;
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

        // path.to.field
        // MYSQL doesn't allow "." in column names
        String columnName = singleNodePath.getFullPath("_");
        String fieldPath = singleNodePath.getFullPath(".");
        Class<?> fieldType = singleNodePath.getLastNodeType();

        String addColumnQuery = MessageFormat.format(
                "ALTER TABLE {0} ADD COLUMN {1} {2} GENERATED ALWAYS AS" +
                        "(JSON_VALUE(`{3}`,'$.{4}'))",
                collectionName, columnName, getColumnType(false, fieldType),
                DATA_IDENTIFIER, fieldPath);

        try (PreparedStatement ps = connection.prepareStatement(addColumnQuery)) {
            ps.execute();
        }
        catch (SQLException e) {
            // TODO better way may be possible
            if(!e.getMessage().toLowerCase().contains("duplicate column name")) {
                throw e;
            }
        }

        // Index this column if not exist
        String createIndexQuery = MessageFormat.format(
                "CREATE INDEX IF NOT EXISTS {0}_index ON {1}({2})",
                columnName, collectionName, columnName);
        try (PreparedStatement ps = connection.prepareStatement(createIndexQuery)) {
            ps.execute();
        }
    }
}
