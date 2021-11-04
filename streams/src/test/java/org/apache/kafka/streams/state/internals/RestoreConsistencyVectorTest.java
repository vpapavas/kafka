/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRocksDbConfigSetter;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@SuppressWarnings({"rawtypes", "unchecked"})
public class RestoreConsistencyVectorTest extends AbstractKeyValueStoreTest {
    final static String DB_NAME = "db-name";
    final static String METRICS_SCOPE = "metrics-scope";

    private File dir;
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();

    InternalMockProcessorContext context;
    RocksDBStore rocksDBStore;

    @Before
    public void setUp() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        rocksDBStore = getRocksDBStore();
    }

    @After
    public void tearDown() {
        rocksDBStore.close();
    }

    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final StateStoreContext context) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("my-store"),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde());

        final KeyValueStore<K, V> store = storeBuilder.build();
        store.init(context, store);
        return store;
    }

    RocksDBStore getRocksDBStore() {
        return new RocksDBStore(DB_NAME, METRICS_SCOPE);
    }


    @Test
    public void shouldRestoreRecordsAndConsistencyVectorSingleTopic() {
        final List<ConsumerRecord<byte[], byte[]>> entries = getChangelogRecords();

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restoreWithHeaders(rocksDBStore.name(), entries);

        assertEquals(
                "a",
                stringDeserializer.deserialize(
                        null,
                        rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        assertEquals(
                "b",
                stringDeserializer.deserialize(
                        null,
                        rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        assertEquals(
                "c",
                stringDeserializer.deserialize(
                        null,
                        rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));

        assertThat(rocksDBStore.getPosition().get(), notNullValue());
        assertThat(rocksDBStore.getPosition().get().getBound(""), notNullValue());
        assertThat(rocksDBStore.getPosition().get().getBound(""), hasEntry(0, 3L));
    }

    @Test
    public void shouldRestoreRecordsAndConsistencyVectorMultipleTopics() {
        final List<ConsumerRecord<byte[], byte[]>> entries = getChangelogRecordsMultipleTopics();

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restoreWithHeaders(rocksDBStore.name(), entries);

        assertEquals(
                "a",
                stringDeserializer.deserialize(
                        null,
                        rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        assertEquals(
                "b",
                stringDeserializer.deserialize(
                        null,
                        rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        assertEquals(
                "c",
                stringDeserializer.deserialize(
                        null,
                        rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));

        assertThat(rocksDBStore.getPosition().get(), notNullValue());
        assertThat(rocksDBStore.getPosition().get().getBound("A"), notNullValue());
        assertThat(rocksDBStore.getPosition().get().getBound("A"), hasEntry(0, 3L));
        assertThat(rocksDBStore.getPosition().get().getBound("B"), notNullValue());
        assertThat(rocksDBStore.getPosition().get().getBound("B"), hasEntry(0, 2L));
    }

    @Test
    public void shouldThrowWhenRestoringOnMissingHeaders() {
        final List<KeyValue<byte[], byte[]>> entries = getChangelogRecordsWithoutHeaders();

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        assertThrows(
                StreamsException.class,
                () -> context.restore(rocksDBStore.name(), entries));
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecords() {
        final List<ConsumerRecord<byte[], byte[]>> entries = new ArrayList<>();
        final Headers headers = new RecordHeaders();
        Position position1 = Position.emptyPosition();
        position1 = position1.update("", 0, 1);

        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(Position.VECTOR_KEY, position1.serialize().array()));
        entries.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                "1".getBytes(UTF_8), "a".getBytes(UTF_8), headers, Optional.empty()));

        headers.remove(Position.VECTOR_KEY);
        position1 = position1.update("", 0, 2);
        headers.add(new RecordHeader(Position.VECTOR_KEY, position1.serialize().array()));
        entries.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                "2".getBytes(UTF_8), "b".getBytes(UTF_8), headers, Optional.empty()));

        headers.remove(Position.VECTOR_KEY);
        position1 = position1.update("", 0, 3);
        headers.add(new RecordHeader(Position.VECTOR_KEY, position1.serialize().array()));
        entries.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                "3".getBytes(UTF_8), "c".getBytes(UTF_8), headers, Optional.empty()));
        return entries;
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecordsMultipleTopics() {
        final List<ConsumerRecord<byte[], byte[]>> entries = new ArrayList<>();
        final Headers headers = new RecordHeaders();
        Position position1 = Position.emptyPosition();

        position1 = position1.update("A", 0, 1);
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(Position.VECTOR_KEY, position1.serialize().array()));
        entries.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                "1".getBytes(UTF_8), "a".getBytes(UTF_8), headers, Optional.empty()));

        headers.remove(Position.VECTOR_KEY);
        position1 = position1.update("B", 0, 2);
        headers.add(new RecordHeader(Position.VECTOR_KEY, position1.serialize().array()));
        entries.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                "2".getBytes(UTF_8), "b".getBytes(UTF_8), headers, Optional.empty()));

        headers.remove(Position.VECTOR_KEY);
        position1 = position1.update("A", 0, 3);
        headers.add(new RecordHeader(Position.VECTOR_KEY, position1.serialize().array()));
        entries.add(new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                "3".getBytes(UTF_8), "c".getBytes(UTF_8), headers, Optional.empty()));
        return entries;
    }

    private List<KeyValue<byte[], byte[]>> getChangelogRecordsWithoutHeaders() {
        final List<KeyValue<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>("1".getBytes(UTF_8), "a".getBytes(UTF_8)));
        entries.add(new KeyValue<>("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
        entries.add(new KeyValue<>("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
        return entries;
    }
}
