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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;

/** Entry of a manifest file, representing an addition / deletion of a data file. */
//todo manifest文件

/***
 * {
 *     "org.apache.paimon.avro.generated.record": {
 *         "_VERSION": 2,
 *         "_KIND": 1,
 *         "_PARTITION":  //分区path"\u0000\u0000\u0000\u0004\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\n\u0000\u0000\u0000(\u0000\u0000\u000001\u0000\u0000\u0000\u0000\u0000kr\u0000\u0000\u0000\u0000\u0000appsdk\u00002023-07-11\u0000\u0000\u0000\u0000\u0000\u0000",
 *         "_BUCKET": 0,
 *         "_TOTAL_BUCKETS": 2,
 *         "_FILE": {
 *             "org.apache.paimon.avro.generated.record__FILE": {
 *                 "_FILE_NAME": "data-71dddb79-0fc3-4bdf-bfae-285ab89dd8a9-0.orc",
 *                 "_FILE_SIZE": 2562,
 *                 "_ROW_COUNT": 4,
 *                 "_MIN_KEY": "\u0000\u0000\u0000\u0004\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000$\u0000\u0000\u0000(\u0000\u0000\u00006ÊS\u0005\u0000\u0000\u0000\u0000netStatL\r¿@\u0001\u0000\u00000502186a03e1ac987380677b7b64ccb99efd\u0000\u0000\u0000\u0000",
 *                 "_MAX_KEY": "\u0000\u0000\u0000\u0004\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000$\u0000\u0000\u0000(\u0000\u0000\u0000\\-Ö\u0005\u0000\u0000\u0000\u0000\u000B\u0000\u0000\u0000P\u0000\u0000\u0000o¿@\u0001\u0000\u00000702ddda315f4e5789b07c68eafe008c7e4a\u0000\u0000\u0000\u0000network_log\u0000\u0000\u0000\u0000\u0000",
 *                 "_KEY_STATS": {
 *                     "org.apache.paimon.avro.generated.record__FILE__KEY_STATS": {
 *                         "_MIN_VALUES": "\u0000\u0000\u0000\u0004\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0010\u0000\u0000\u0000(\u0000\u0000\u0000þl\u0002\u0000\u0000\u0000\u0000netStatL\r¿@\u0001\u0000\u00000502186a03e1ac98",
 *                         "_MAX_VALUES": "\u0000\u0000\u0000\u0004\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0010\u0000\u0000\u0000(\u0000\u0000\u0000\\-Ö\u0005\u0000\u0000\u0000\u0000\u000B\u0000\u0000\u00008\u0000\u0000\u0000(À@\u0001\u0000\u00000702ddda315f4e58network_log\u0000\u0000\u0000\u0000\u0000",
 *                         "_NULL_COUNTS": {
 *                             "array": [{
 *                                 "long": 0
 *                             }, {
 *                                 "long": 0
 *                             }, {
 *                                 "long": 0
 *                             }, {
 *                                 "long": 0
 *                             }]
 *                         }
 *                     }
 *                 },
 *                 "_VALUE_STATS": {
 *                     "org.apache.paimon.avro.generated.record__FILE__VALUE_STATS": {
 *                         "_MIN_VALUES": "\u0000\u0000\u0000\n\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000þl\u0002\u0000\u0000\u0000\u0000netStattrack\u0000\u0000
 *                         L\ r¿ @\ u0001\ u0000\ u0000ÛE¿ @\ u0001\ u0000\ u0000\ u0010\ u0000\ u0000\ u0000X\ u0000\ u0000\ u0000\ n\ u0000\ u0000\ u0000h\ u0000\ u0000\ u000001\ u0000\ u0000\ u0000\ u0000\ u0000kr\ u0000\ u0000\ u0000\ u0000\ u0000appsdk\ u00000502186a03e1ac982023 - 07 - 11\ u0000\ u0000\ u0000\ u0000\ u0000\ u0000 ","
 *                         _MAX_VALUES ":"\
 *                         u0000\ u0000\ u0000\ n\ u0000\ u0000\ u0000\ u0000\ u0000\ u0000\ u0000\ u0000\\ - Ö\ u0005\ u0000\ u0000\ u0000\ u0000\ u000B\ u0000\ u0000\ u0000X\ u0000\ u0000\ u0000track\ u0000\ u0000(À @\ u0001\ u0000\ u0000· JÀ @\ u0001\ u0000\ u0000\ u0010\ u0000\ u0000\ u0000h\ u0000\ u0000\ u0000\ n\ u0000\ u0000\ u0000x\ u0000\ u0000\ u000001\ u0000\ u0000\ u0000\ u0000\ u0000kr\ u0000\ u0000\ u0000\ u0000\ u0000appsdk\ u0000network_log\ u0000\ u0000\ u0000\ u0000\ u00000702ddda315f4e582023 - 07 - 11\ u0000\ u0000\ u0000\ u0000\ u0000\ u0000 ","
 *                             _NULL_COUNTS ":{"
 *                             array ":[{"
 *                             long ":0},{"
 *                             long ":0},{"
 *                             long ":0},{"
 *                             long ":0},{"
 *                             long ":0},{"
 *                             long ":0},{"
 *                             long ":0},{"
 *                             long ":0},{"
 *                             long ":0},{"
 *                             long ":0}]}}},"
 *                             _MIN_SEQUENCE_NUMBER ":1689008424411,"
 *                             _MAX_SEQUENCE_NUMBER ":1689008491191,"
 *                             _SCHEMA_ID ":0,"
 *                             _LEVEL ":0,"
 *                             _EXTRA_FILES ":[],"
 *                             _CREATION_TIME ":{"
 *                             long ":1689008594769
 *                     }
 *                 }
 *             }
 *         }
 *     }
 */

//todo manifest文件
public class ManifestEntry {

    private final FileKind kind;
    // for tables without partition this field should be a row with 0 columns (not null)
    private final BinaryRow partition;
    private final int bucket;
    private final int totalBuckets;
    private final DataFileMeta file;

    public ManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, int totalBuckets, DataFileMeta file) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.file = file;
    }

    public FileKind kind() {
        return kind;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public DataFileMeta file() {
        return file;
    }

    public Identifier identifier() {
        return new Identifier(partition, bucket, file.level(), file.fileName());
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_KIND", new TinyIntType(false)));
        fields.add(new DataField(1, "_PARTITION", newBytesType(false)));
        fields.add(new DataField(2, "_BUCKET", new IntType(false)));
        fields.add(new DataField(3, "_TOTAL_BUCKETS", new IntType(false)));
        fields.add(new DataField(4, "_FILE", DataFileMeta.schema()));
        return new RowType(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestEntry)) {
            return false;
        }
        ManifestEntry that = (ManifestEntry) o;
        return Objects.equals(kind, that.kind)
                && Objects.equals(partition, that.partition)
                && bucket == that.bucket
                && totalBuckets == that.totalBuckets
                && Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, totalBuckets, file);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d, %d, %s}", kind, partition, bucket, totalBuckets, file);
    }

    public static Collection<ManifestEntry> mergeEntries(Iterable<ManifestEntry> entries) {
        LinkedHashMap<Identifier, ManifestEntry> map = new LinkedHashMap<>();
        mergeEntries(entries, map);
        return map.values();
    }

    public static void mergeEntries(
            Iterable<ManifestEntry> entries, Map<Identifier, ManifestEntry> map) {
        //todo 迭代遍历了！！！！！！
        for (ManifestEntry entry : entries) {
            ManifestEntry.Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    Preconditions.checkState(
                            !map.containsKey(identifier),
                            "Trying to add file %s which is already added. Manifest might be corrupted.",
                            identifier);
                    map.put(identifier, entry);
                    break;
                case DELETE:
                    // each dataFile will only be added once and deleted once,
                    // if we know that it is added before then both add and delete entry can be
                    // removed because there won't be further operations on this file,
                    // otherwise we have to keep the delete entry because the add entry must be
                    // in the previous manifest files
                    if (map.containsKey(identifier)) {
                        map.remove(identifier);
                    } else {
                        map.put(identifier, entry);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
    }

    public static void assertNoDelete(Collection<ManifestEntry> entries) {
        for (ManifestEntry entry : entries) {
            Preconditions.checkState(
                    entry.kind() != FileKind.DELETE,
                    "Trying to delete file %s which is not previously added. Manifest might be corrupted.",
                    entry.file().fileName());
        }
    }

    /**
     * The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data
     * file.
     */
    public static class Identifier {
        public final BinaryRow partition;
        public final int bucket;
        public final int level;
        public final String fileName;

        /* Cache the hash code for the string */
        private Integer hash;

        private Identifier(BinaryRow partition, int bucket, int level, String fileName) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Identifier)) {
                return false;
            }
            Identifier that = (Identifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level
                    && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash = Objects.hash(partition, bucket, level, fileName);
            }
            return hash;
        }

        @Override
        public String toString() {
            return String.format("{%s, %d, %d, %s}", partition, bucket, level, fileName);
        }

        public String toString(FileStorePathFactory pathFactory) {
            return pathFactory.getPartitionString(partition)
                    + ", bucket "
                    + bucket
                    + ", level "
                    + level
                    + ", file "
                    + fileName;
        }
    }
}
