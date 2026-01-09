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

package org.apache.flink.cdc.connectors.gaussdb.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBDialect;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.events.OffsetCommitAckEvent;
import org.apache.flink.cdc.connectors.gaussdb.source.events.OffsetCommitEvent;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigning;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

/**
 * The GaussDB source enumerator that enumerates receive the split request and
 * assign the split to
 * source readers.
 */
@Internal
public class GaussDBSourceEnumerator extends IncrementalSourceEnumerator {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBSourceEnumerator.class);

    private final GaussDBDialect gaussDialect;
    private final GaussDBSourceConfig sourceConfig;
    private volatile boolean receiveOffsetCommitAck = false;

    public GaussDBSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            GaussDBSourceConfig sourceConfig,
            SplitAssigner splitAssigner,
            GaussDBDialect gaussDialect,
            Boundedness boundedness) {
        super(context, sourceConfig, splitAssigner, boundedness);
        this.gaussDialect = gaussDialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void start() {
        createSlotForGlobalStreamSplit();
        super.start();
    }

    @Override
    protected void assignSplits() {
        // if scan newly added table is enable, can not assign new added table's
        // snapshot splits
        // until source reader doesn't commit offset.
        if (sourceConfig.isScanNewlyAddedTableEnabled()
                && streamSplitTaskId != null
                && !receiveOffsetCommitAck
                && isNewlyAddedAssigning(splitAssigner.getAssignerStatus())) {
            // just return here, the reader has been put into readersAwaitingSplit, will be
            // assigned
            // split again later
            return;
        }
        super.assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof OffsetCommitAckEvent) {
            if (streamSplitTaskId != null && streamSplitTaskId == subtaskId) {
                this.receiveOffsetCommitAck = true;
            } else {
                throw new RuntimeException("Receive SyncAssignStatusAck from wrong subtask");
            }
        } else {
            super.handleSourceEvent(subtaskId, sourceEvent);
        }
    }

    @Override
    protected void syncWithReaders(int[] subtaskIds, Throwable t) {
        super.syncWithReaders(subtaskIds, t);
        // if scan newly added table is enable, gaussdb enumerator will send its
        // OffsetCommitEvent
        // to tell reader whether to start offset commit.
        if (!receiveOffsetCommitAck
                && sourceConfig.isScanNewlyAddedTableEnabled()
                && streamSplitTaskId != null) {
            AssignerStatus assignerStatus = splitAssigner.getAssignerStatus();
            context.sendEventToSourceReader(
                    streamSplitTaskId,
                    new OffsetCommitEvent(
                            !isNewlyAddedAssigning(assignerStatus)
                                    && !isNewlyAddedAssigningSnapshotFinished(assignerStatus)));
        }
    }

    /**
     * Create slot for the unique global stream split.
     *
     * <p>
     * Currently all startup modes need read the stream split. We need open the slot
     * before
     * reading the globalStreamSplit to catch all data changes.
     */
    private void createSlotForGlobalStreamSplit() {
        String slotName = gaussDialect.getSlotName();
        String pluginName = gaussDialect.getPluginName();

        LOG.info("Checking replication slot: slotName={}, pluginName={}", slotName, pluginName);

        try (JdbcConnection connection = gaussDialect.openJdbcConnection(sourceConfig)) {
            // Check if slot exists using SQL query
            boolean slotExists = checkSlotExists(connection, slotName);

            if (slotExists) {
                LOG.info("Replication slot '{}' already exists, skipping creation.", slotName);
                return;
            }

            // Create the replication slot using SQL
            LOG.info("Creating replication slot: {}", slotName);
            String createSlotSql = String.format(
                    "SELECT * FROM pg_create_logical_replication_slot('%s', '%s')",
                    slotName, pluginName);
            connection.execute(createSlotSql);
            LOG.info("Replication slot '{}' created successfully.", slotName);

        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to get or create replication slot '%s' with plugin '%s'",
                            slotName, pluginName),
                    t);
        }
    }

    private boolean checkSlotExists(JdbcConnection connection, String slotName) {
        try {
            final boolean[] exists = { false };
            String checkSql = String.format(
                    "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '%s'",
                    slotName);
            connection.query(checkSql, rs -> {
                if (rs.next()) {
                    exists[0] = true;
                }
            });
            return exists[0];
        } catch (Exception e) {
            LOG.warn("Error checking if replication slot exists: {}", e.getMessage());
            return false;
        }
    }
}
