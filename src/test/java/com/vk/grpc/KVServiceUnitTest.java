package com.vk.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.*;
import vk.grpc.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class KVServiceUnitTest {

    private static KeyValueServiceGrpc.KeyValueServiceBlockingStub stub;
    private static Server server;
    private static ManagedChannel channel;
    private static String grpcServiceName;

    static class MockRepository extends KVRepository {
        private final Map<String, byte[]> store = new HashMap<>();

        public MockRepository() {
            super(null);
        }

        @Override
        public void save(KVRecord record) {
            store.put(record.key(), record.value());
        }

        @Override
        public Optional<KVRecord> findByKey(String key) {
            if (!store.containsKey(key)) {
                return Optional.empty();
            }
            byte[] value = store.get(key);
            return Optional.of(new KVRecord(key, value));
        }

        @Override
        public boolean deleteByKey(String key) {
            return store.remove(key) != null;
        }

        @Override
        public void streamRange(String keySince, String keyTo, int batchSize, java.util.function.Consumer<KVRecord> consumer) {
            store.entrySet().stream()
                    .filter(e -> e.getKey().compareTo(keySince) >= 0 && e.getKey().compareTo(keyTo) < 0)
                    .sorted(Map.Entry.comparingByKey())
                    .limit(batchSize)
                    .forEach(e -> consumer.accept(new KVRecord(e.getKey(), e.getValue())));
        }

        @Override
        public long countAll() {
            return store.size();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        grpcServiceName = InProcessServerBuilder.generateName();

        var repository = new MockRepository();
        var service = new KVServiceImpl(repository);

        server = InProcessServerBuilder.forName(grpcServiceName)
                .directExecutor()
                .addService(service)
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(grpcServiceName)
                .directExecutor()
                .build();

        stub = KeyValueServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdownNow();
        }
        if (server != null && !server.isShutdown()) {
            server.shutdownNow();
        }
    }

    @Test
    void putAndGet_shouldWork() {
        stub.put(PutRequest.newBuilder()
                .setKey("user:1")
                .setValue(ByteString.copyFromUtf8("Alice"))
                .build());

        var resp = stub.get(GetRequest.newBuilder().setKey("user:1").build());
        assertTrue(resp.getSuccess());
        assertEquals("Alice", resp.getValue().toStringUtf8());
    }

    @Test
    void putAndGet_withNullValue_shouldWork() {
        stub.put(PutRequest.newBuilder()
                .setKey("config:dark_mode")
                .setValue(ByteString.EMPTY)
                .build());

        var resp = stub.get(GetRequest.newBuilder().setKey("config:dark_mode").build());
        assertTrue(resp.getSuccess());
        assertTrue(resp.getValue().isEmpty(), "null должен возвращаться как пустой ByteString");
    }

    @Test
    void delete_shouldRemoveRecord() {
        stub.put(PutRequest.newBuilder()
                .setKey("temp")
                .setValue(ByteString.copyFromUtf8("data"))
                .build());

        var delResp = stub.delete(DeleteRequest.newBuilder().setKey("temp").build());
        assertTrue(delResp.getSuccess());

        var getResp = stub.get(GetRequest.newBuilder().setKey("temp").build());
        assertFalse(getResp.getSuccess(), "Удалённая запись не должна находиться");
    }

    @Test
    void range_shouldStreamInOrder() {
        for (int i = 1; i <= 5; i++) {
            stub.put(PutRequest.newBuilder()
                    .setKey("key_" + i)
                    .setValue(ByteString.copyFromUtf8("val_" + i))
                    .build());
        }

        List<KeyValuePair> results = new ArrayList<>();
        var iterator = stub.range(RangeRequest.newBuilder()
                .setKeySince("key_1")
                .setKeyTo("key_9")
                .build());
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }

        assertEquals(5, results.size());
        assertEquals("key_1", results.get(0).getKey());
        assertEquals("val_5", results.get(4).getValue().toStringUtf8());
    }

    @Test
    void count_shouldReturnExactNumber() {
        assertEquals(0, stub.count(CountRequest.getDefaultInstance()).getCount());

        for (int i = 0; i < 100; i++) {
            stub.put(PutRequest.newBuilder()
                    .setKey("item_" + i)
                    .setValue(ByteString.copyFromUtf8("data"))
                    .build());
        }

        assertEquals(100, stub.count(CountRequest.getDefaultInstance()).getCount());
    }

    @Test
    void get_nonExistentKey_shouldReturnSuccessFalse() {
        var resp = stub.get(GetRequest.newBuilder().setKey("missing").build());
        assertFalse(resp.getSuccess());
        assertTrue(resp.getValue().isEmpty());
    }

    @Test
    void range_withEmptyResult_shouldReturnEmptyStream() {
        var iterator = stub.range(RangeRequest.newBuilder()
                .setKeySince("a")
                .setKeyTo("z")
                .build());

        assertFalse(iterator.hasNext(), "Пустой диапазон не должен возвращать элементы");
    }
}