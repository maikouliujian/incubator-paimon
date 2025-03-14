/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.operation;

import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.utils.RecordWriter;

import java.util.List;

/**
 * Write operation which provides {@link RecordWriter} creation and writes {@link SinkRecord} to
 * {@link FileStore}.
 *
 * @param <T> type of record to write.
 */
public interface FileStoreWrite<T> {

    FileStoreWrite<T> withIOManager(IOManager ioManager);

    /**
     * With memory pool for the current file store write.
     *
     * @param memoryPool the given memory pool.
     */
    default FileStoreWrite<T> withMemoryPool(MemorySegmentPool memoryPool) {
        return withMemoryPoolFactory(new MemoryPoolFactory(memoryPool));
    }

    /**
     * With memory pool factory for the current file store write.
     *
     * @param memoryPoolFactory the given memory pool factory.
     */
    FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory);

    /**
     * Set whether the write operation should ignore previously stored files.
     *
     * @param ignorePreviousFiles whether the write operation should ignore previously stored files.
     */
    void withIgnorePreviousFiles(boolean ignorePreviousFiles);

    /**
     * Write the data to the store according to the partition and bucket.
     *
     * @param partition the partition of the data
     * @param bucket the bucket id of the data
     * @param data the given data
     * @throws Exception the thrown exception when writing the record
     */
    void write(BinaryRow partition, int bucket, T data) throws Exception;

    /**
     * Compact data stored in given partition and bucket. Note that compaction process is only
     * submitted and may not be completed when the method returns.
     *
     * @param partition the partition to compact
     * @param bucket the bucket to compact
     * @param fullCompaction whether to trigger full compaction or just normal compaction
     * @throws Exception the thrown exception when compacting the records
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * Notify that some new files are created at given snapshot in given bucket.
     *
     * <p>Most probably, these files are created by another job. Currently this method is only used
     * by the dedicated compact job to see files created by writer jobs.
     *
     * @param snapshotId the snapshot id where new files are created
     * @param partition the partition where new files are created
     * @param bucket the bucket where new files are created
     * @param files the new files themselves
     */
    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    /**
     * Prepare commit in the write.
     *
     * @param waitCompaction if this method need to wait for current compaction to complete
     * @param commitIdentifier identifier of the commit being prepared
     * @return the file committable list
     * @throws Exception the thrown exception
     */
    List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception;

    /**
     * We detect whether it is in batch mode, if so, we do some optimization.
     *
     * @param isStreamingMode whether in streaming mode
     */
    void isStreamingMode(boolean isStreamingMode);

    /**
     * Close the writer.
     *
     * @throws Exception the thrown exception
     */
    void close() throws Exception;
}
