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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.NewFilesIncrement;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link RecordWriter} to write records and generate {@link CompactIncrement}. */
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {

    private final boolean writeBufferSpillable;
    private final int sortMaxFan;
    private final IOManager ioManager;

    private final RowType keyType;
    private final RowType valueType;
    private final CompactManager compactManager;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunction<KeyValue> mergeFunction;
    private final KeyValueFileWriterFactory writerFactory;
    private final boolean commitForceCompact;
    private final ChangelogProducer changelogProducer;
    //todo 本次ckp产生的新data文件
    private final LinkedHashSet<DataFileMeta> newFiles;
    //todo 本次ckp产生的新changelog文件
    private final LinkedHashSet<DataFileMeta> newFilesChangelog;
    //todo 周期compaction产生的compactBefore data文件
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    //todo 周期compaction产生的compactAfter data文件
    private final LinkedHashSet<DataFileMeta> compactAfter;
    //todo 周期compaction产生的changelog文件
    private final LinkedHashSet<DataFileMeta> compactChangelog;

    private long newSequenceNumber;
    private WriteBuffer writeBuffer;

    public MergeTreeWriter(
            boolean writeBufferSpillable,
            int sortMaxFan,
            IOManager ioManager,
            CompactManager compactManager,
            long maxSequenceNumber,
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            ChangelogProducer changelogProducer,
            @Nullable CommitIncrement increment) {
        this.writeBufferSpillable = writeBufferSpillable;
        this.sortMaxFan = sortMaxFan;
        this.ioManager = ioManager;
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.changelogProducer = changelogProducer;

        this.newFiles = new LinkedHashSet<>();
        this.newFilesChangelog = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.compactChangelog = new LinkedHashSet<>();
        //todo 第一次启动increment == null；如果从checkpoint/savepoint恢复，increment != null
        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            newFilesChangelog.addAll(increment.newFilesIncrement().changelogFiles());
            increment
                    .compactIncrement()
                    .compactBefore()
                    .forEach(f -> compactBefore.put(f.fileName(), f));
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            compactChangelog.addAll(increment.compactIncrement().changelogFiles());
        }
    }

    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    @VisibleForTesting
    CompactManager compactManager() {
        return compactManager;
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.writeBuffer =
                new SortBufferWriteBuffer(
                        keyType,
                        valueType,
                        memoryPool,
                        writeBufferSpillable,
                        sortMaxFan,
                        ioManager);
    }
    //todo 写入数据！！！！！！
    @Override
    public void write(KeyValue kv) throws Exception {
        long sequenceNumber =
                kv.sequenceNumber() == KeyValue.UNKNOWN_SEQUENCE
                        ? newSequenceNumber()
                        : kv.sequenceNumber();
        //todo 内存不够，写入失败，返回false
        boolean success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
        if (!success) {
            //todo 先刷写磁盘！！！！！！
            flushWriteBuffer(false, false);
            //todo 再写入内存
            success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flushWriteBuffer(true, fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    @Override
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        boolean success = writeBuffer.flushMemory();
        if (!success) {
            flushWriteBuffer(false, false);
        }
    }
    //todo 内存刷写磁盘！！！！！！
    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        if (writeBuffer.size() > 0) {
            //todo 是否需要触发合并
            if (compactManager.shouldWaitForLatestCompaction()) {
                waitForLatestCompaction = true;
            }
            //todo 如果changelogProducer为input，则需要直接写changelog，否则不写changelog
            //todo 写时，数据都是在0层级
            final RollingFileWriter<KeyValue, DataFileMeta> changelogWriter =
                    changelogProducer == ChangelogProducer.INPUT
                            ? writerFactory.createRollingChangelogFileWriter(0)
                            : null;
            final RollingFileWriter<KeyValue, DataFileMeta> dataWriter =
                    writerFactory.createRollingMergeTreeFileWriter(0);

            try {
                //todo 写数据逻辑的定义！！！
                writeBuffer.forEach(
                        keyComparator,
                        mergeFunction,
                        //todo changelogWriter写的是全部的数据文件，所有的变化都会记录下来
                        changelogWriter == null ? null : changelogWriter::write,
                        //todo dataWriter写的是merge后的数据文件
                        dataWriter::write);
            } finally {
                if (changelogWriter != null) {
                    changelogWriter.close();
                }
                dataWriter.close();
            }

            if (changelogWriter != null) {
                //todo 更新Changelog manifest
                newFilesChangelog.addAll(changelogWriter.result());
            }

            for (DataFileMeta fileMeta : dataWriter.result()) {
                //todo 更新data manifest
                newFiles.add(fileMeta);
                //todo 将data manifest更新到lsm树的0层
                compactManager.addNewFile(fileMeta);
            }

            writeBuffer.clear();
        }
        //todo 更新compaction结果
        trySyncLatestCompaction(waitForLatestCompaction);
        compactManager.triggerCompaction(forcedFullCompaction);
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        flushWriteBuffer(waitCompaction, false);
        //todo 更新compaction结果
        trySyncLatestCompaction(
                waitCompaction
                        || commitForceCompact
                        || compactManager.shouldWaitForPreparingCheckpoint());
        //todo 处理各类文件
        return drainIncrement();
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    private CommitIncrement drainIncrement() {
        NewFilesIncrement newFilesIncrement =
                new NewFilesIncrement(
                        new ArrayList<>(newFiles), new ArrayList<>(newFilesChangelog));
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter),
                        new ArrayList<>(compactChangelog));

        newFiles.clear();
        newFilesChangelog.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactChangelog.clear();

        return new CommitIncrement(newFilesIncrement, compactIncrement);
    }

    //todo compaction结束后，会更新CompactResult
    private void updateCompactResult(CompactResult result) {
        Set<String> afterFiles =
                result.after().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
        for (DataFileMeta file : result.before()) {
            if (compactAfter.remove(file)) {
                // This is an intermediate file (not a new data file), which is no longer needed
                // after compaction and can be deleted directly, but upgrade file is required by
                // previous snapshot and following snapshot, so we should ensure:
                // 1. This file is not the output of upgraded.
                // 2. This file is not the input of upgraded.
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    writerFactory.deleteFile(file.fileName(), file.level());
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after());
        compactChangelog.addAll(result.changelog());
    }

    private void trySyncLatestCompaction(boolean blocking) throws Exception {
        //todo 获取compation的结果！！！
        Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
        result.ifPresent(this::updateCompactResult);
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();
        compactManager.close();

        // delete temporary files
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        newFiles.clear();

        for (DataFileMeta file : newFilesChangelog) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
        newFilesChangelog.clear();

        for (DataFileMeta file : compactAfter) {
            // upgrade file is required by previous snapshot, so we should ensure that this file is
            // not the output of upgraded.
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }

        compactAfter.clear();

        for (DataFileMeta file : compactChangelog) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
        compactChangelog.clear();

        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
    }
}
