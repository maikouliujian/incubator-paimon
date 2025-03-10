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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.paimon.operation.FileStoreScan.Plan.groupByPartFiles;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** Implementation of {@link SnapshotReader}. */
public class SnapshotReaderImpl implements SnapshotReader {

    private final FileStoreScan scan;
    private final TableSchema tableSchema;
    private final CoreOptions options;
    private final SnapshotManager snapshotManager;
    private final ConsumerManager consumerManager;
    private final SplitGenerator splitGenerator;
    private final BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer;
    private final DefaultValueAssigner defaultValueAssigner;

    private ScanKind scanKind = ScanKind.ALL;
    private RecordComparator lazyPartitionComparator;

    public SnapshotReaderImpl(
            FileStoreScan scan,
            TableSchema tableSchema,
            CoreOptions options,
            SnapshotManager snapshotManager,
            SplitGenerator splitGenerator,
            BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer,
            DefaultValueAssigner defaultValueAssigner) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.options = options;
        this.snapshotManager = snapshotManager;
        this.consumerManager =
                new ConsumerManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.splitGenerator = splitGenerator;
        this.nonPartitionFilterConsumer = nonPartitionFilterConsumer;
        this.defaultValueAssigner = defaultValueAssigner;
    }

    @Override
    public SnapshotManager snapshotManager() {
        return snapshotManager;
    }

    @Override
    public ConsumerManager consumerManager() {
        return consumerManager;
    }

    @Override
    public SplitGenerator splitGenerator() {
        return splitGenerator;
    }

    @Override
    public SnapshotReader withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
        return this;
    }

    @Override
    public SnapshotReader withSnapshot(Snapshot snapshot) {
        scan.withSnapshot(snapshot);
        return this;
    }

    @Override
    public SnapshotReader withFilter(Predicate predicate) {
        List<String> partitionKeys = tableSchema.partitionKeys();
        int[] fieldIdxToPartitionIdx =
                tableSchema.fields().stream()
                        .mapToInt(f -> partitionKeys.indexOf(f.name()))
                        .toArray();

        List<Predicate> partitionFilters = new ArrayList<>();
        List<Predicate> nonPartitionFilters = new ArrayList<>();
        for (Predicate p :
                PredicateBuilder.splitAnd(defaultValueAssigner.handlePredicate(predicate))) {
            Optional<Predicate> mapped = transformFieldMapping(p, fieldIdxToPartitionIdx);
            if (mapped.isPresent()) {
                partitionFilters.add(mapped.get());
            } else {
                nonPartitionFilters.add(p);
            }
        }

        if (partitionFilters.size() > 0) {
            scan.withPartitionFilter(PredicateBuilder.and(partitionFilters));
        }

        if (nonPartitionFilters.size() > 0) {
            nonPartitionFilterConsumer.accept(scan, PredicateBuilder.and(nonPartitionFilters));
        }
        return this;
    }

    @Override
    public SnapshotReader withKind(ScanKind scanKind) {
        this.scanKind = scanKind;
        scan.withKind(scanKind);
        return this;
    }

    @Override
    public SnapshotReader withLevelFilter(Filter<Integer> levelFilter) {
        scan.withLevelFilter(levelFilter);
        return this;
    }

    @Override
    public SnapshotReader withBucket(int bucket) {
        scan.withBucket(bucket);
        return this;
    }

    /** Get splits from {@link FileKind#ADD} files. */
    @Override
    public Plan read() {
        FileStoreScan.Plan plan = scan.plan();
        Long snapshotId = plan.snapshotId();

        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> files =
                groupByPartFiles(plan.files(FileKind.ADD));
        if (options.scanPlanSortPartition()) {
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> newFiles = new LinkedHashMap<>();
            files.entrySet().stream()
                    .sorted((o1, o2) -> partitionComparator().compare(o1.getKey(), o2.getKey()))
                    .forEach(entry -> newFiles.put(entry.getKey(), entry.getValue()));
            files = newFiles;
        }
        List<DataSplit> splits =
                generateSplits(
                        snapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : snapshotId,
                        scanKind != ScanKind.ALL,
                        splitGenerator,
                        files);
        return new Plan() {
            @Nullable
            @Override
            public Long watermark() {
                return plan.watermark();
            }

            @Nullable
            @Override
            public Long snapshotId() {
                return plan.snapshotId();
            }

            @Override
            public List<Split> splits() {
                return (List) splits;
            }
        };
    }

    @Override
    public List<BinaryRow> partitions() {
        List<ManifestEntry> entryList = scan.plan().files();

        return entryList.stream()
                .collect(
                        Collectors.groupingBy(
                                ManifestEntry::partition,
                                LinkedHashMap::new,
                                Collectors.reducing((a, b) -> b)))
                .values()
                .stream()
                .map(Optional::get)
                .map(ManifestEntry::partition)
                .collect(Collectors.toList());
    }

    /** Get splits from an overwritten snapshot files. */
    @Override
    public Plan readOverwrittenChanges() {
        withKind(ScanKind.DELTA);
        FileStoreScan.Plan plan = scan.plan();
        long snapshotId = plan.snapshotId();

        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        if (snapshot.commitKind() != Snapshot.CommitKind.OVERWRITE) {
            throw new IllegalStateException(
                    "Cannot read overwrite splits from a non-overwrite snapshot.");
        }

        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles =
                groupByPartFiles(plan.files(FileKind.DELETE));
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));

        return toChangesPlan(true, plan, beforeFiles, dataFiles);
    }

    private Plan toChangesPlan(
            boolean isStreaming,
            FileStoreScan.Plan plan,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles) {
        List<DataSplit> splits = new ArrayList<>();
        Map<BinaryRow, Set<Integer>> buckets = new HashMap<>();
        beforeFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        dataFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));

        for (Map.Entry<BinaryRow, Set<Integer>> entry : buckets.entrySet()) {
            BinaryRow part = entry.getKey();
            for (Integer bucket : entry.getValue()) {
                List<DataFileMeta> before =
                        beforeFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());
                List<DataFileMeta> data =
                        dataFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());

                // deduplicate
                before.removeIf(data::remove);

                DataSplit split =
                        DataSplit.builder()
                                .withSnapshot(plan.snapshotId())
                                .withPartition(part)
                                .withBucket(bucket)
                                .withBeforeFiles(before)
                                .withDataFiles(data)
                                .isStreaming(isStreaming)
                                .build();
                splits.add(split);
            }
        }

        return new Plan() {
            @Nullable
            @Override
            public Long watermark() {
                return plan.watermark();
            }

            @Nullable
            @Override
            public Long snapshotId() {
                return plan.snapshotId();
            }

            @Override
            public List<Split> splits() {
                return (List) splits;
            }
        };
    }

    @Override
    public Plan readIncrementalDiff(Snapshot before) {
        withKind(ScanKind.ALL);
        FileStoreScan.Plan plan = scan.plan();
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles =
                groupByPartFiles(scan.withSnapshot(before).plan().files(FileKind.ADD));
        return toChangesPlan(false, plan, beforeFiles, dataFiles);
    }

    private RecordComparator partitionComparator() {
        if (lazyPartitionComparator == null) {
            lazyPartitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes(),
                            "PartitionComparator");
        }
        return lazyPartitionComparator;
    }

    @VisibleForTesting
    public static List<DataSplit> generateSplits(
            long snapshotId,
            boolean isStreaming,
            SplitGenerator splitGenerator,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupedDataFiles) {
        List<DataSplit> splits = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entry :
                groupedDataFiles.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<DataFileMeta>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                List<DataFileMeta> bucketFiles = bucketEntry.getValue();
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(snapshotId)
                                .withPartition(partition)
                                .withBucket(bucket)
                                .isStreaming(isStreaming);
                List<List<DataFileMeta>> splitGroups =
                        isStreaming
                                ? splitGenerator.splitForStreaming(bucketFiles)
                                : splitGenerator.splitForBatch(bucketFiles);
                splitGroups.stream()
                        .map(builder::withDataFiles)
                        .map(DataSplit.Builder::build)
                        .forEach(splits::add);
            }
        }
        return splits;
    }
}
