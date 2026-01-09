/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBScanFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBStreamFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.CustomGaussDBSchema;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.TableDiscoveryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;

/**
 * The dialect for GaussDB.
 * Uses JdbcConnection directly (not PostgresConnection) to bypass PostgreSQL
 * version check.
 */
public class GaussDBDialect implements JdbcDataSourceDialect {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBDialect.class);
    private static final long serialVersionUID = 1L;
    private static final String CONNECTION_NAME = "gaussdb-cdc-connector";

    private final GaussDBSourceConfig sourceConfig;
    private transient Tables.TableFilter filters;
    private transient CustomGaussDBSchema schema;
    private transient GaussDBStreamFetchTask streamFetchTask;

    public GaussDBDialect(GaussDBSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "GaussDB";
    }

    /**
     * Opens a JDBC connection using JdbcConnection directly (not
     * PostgresConnection),
     * bypassing the PostgreSQL version check that would fail for GaussDB.
     */
    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        LOG.info("=== GaussDBDialect.openJdbcConnection STARTED ===");
        try {
            GaussDBSourceConfig gaussDBSourceConfig = (GaussDBSourceConfig) sourceConfig;
            LOG.info(
                    "Creating JDBC connection to GaussDB: hostname={}, port={}, database={}",
                    gaussDBSourceConfig.getHostname(),
                    gaussDBSourceConfig.getPort(),
                    gaussDBSourceConfig.getDatabaseList());

            // Use JdbcConnection directly, not PostgresConnection
            // This bypasses the PostgreSQL version check (9.4+ requirement)
            JdbcConnection jdbcConnection = new JdbcConnection(
                    gaussDBSourceConfig.getDbzConnectorConfig().getJdbcConfig(),
                    new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()),
                    "\"",
                    "\"");
            jdbcConnection.connect();
            LOG.info("=== JDBC connection established successfully ===");
            return jdbcConnection;
        } catch (Exception e) {
            LOG.error("=== Failed to open JDBC connection ===", e);
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new GaussDBConnectionPoolFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        LOG.info("=== GaussDBDialect.discoverDataCollections STARTED ===");
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            List<TableId> tables = TableDiscoveryUtils.listTables(
                    null,
                    jdbc,
                    ((GaussDBSourceConfig) sourceConfig).getTableFilters());
            LOG.info("=== Discovered {} tables: {} ===", tables.size(), tables);
            return tables;
        } catch (SQLException e) {
            LOG.error("=== Failed to discover tables ===", e);
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        LOG.info("=== GaussDBDialect.discoverDataCollectionSchemas STARTED ===");
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);
        LOG.info("Fetching schemas for {} tables", capturedTableIds.size());

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            // fetch table schemas
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChange tableSchema = queryTableSchema(jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            LOG.info("=== Successfully fetched {} table schemas ===", tableSchemas.size());
            return tableSchemas;
        } catch (Exception e) {
            LOG.error("=== Failed to discover table schemas ===", e);
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (schema == null) {
            schema = new CustomGaussDBSchema(jdbc);
        }
        return schema.getTableSchema(tableId);
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return currentOffset(jdbc);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    private GaussDBOffset currentOffset(JdbcConnection jdbc) {
        try {
            final String[] result = new String[1];
            jdbc.query(
                    "SELECT pg_current_xlog_location()",
                    rs -> {
                        if (rs.next()) {
                            result[0] = rs.getString(1);
                        }
                    });

            if (result[0] == null) {
                throw new FlinkRuntimeException("Could not get current xlog location");
            }

            LOG.debug("Current GaussDB LSN: {}", result[0]);
            // Create a map-based offset
            Map<String, String> offsetMap = new HashMap<>();
            offsetMap.put("lsn", result[0]);
            return GaussDBOffset.of(offsetMap);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to get current offset", e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // GaussDB/PostgreSQL is case insensitive except when double-quoted
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new GaussDBChunkSplitter(sourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new GaussDBScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = new GaussDBStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    @Override
    public JdbcSourceFetchTaskContext createFetchTaskContext(JdbcSourceConfig taskSourceConfig) {
        return new GaussDBSourceFetchTaskContext((GaussDBSourceConfig) taskSourceConfig, this);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        if (streamFetchTask != null) {
            streamFetchTask.commitCurrentOffset(offset);
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        if (filters == null) {
            this.filters = sourceConfig.getTableFilters().dataCollectionFilter();
        }
        return filters.isIncluded(tableId);
    }

    public String getSlotName() {
        return sourceConfig.getDbzProperties().getProperty(SLOT_NAME.name());
    }

    public String getPluginName() {
        return sourceConfig.getDbzProperties().getProperty(PLUGIN_NAME.name());
    }

    public GaussDBSourceConfig getSourceConfig() {
        return sourceConfig;
    }
}
