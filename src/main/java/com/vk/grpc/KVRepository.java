package com.vk.grpc;

import com.google.protobuf.ByteString;
import io.tarantool.client.box.TarantoolBoxClient;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class KVRepository {
    private final TarantoolBoxClient client;
    private static final String SPACE_NAME = "KV";

    public KVRepository(TarantoolBoxClient client) {
        this.client = client;
    }

    public void save(KVRecord record) {
        var tuple = KVMapper.toTuple(record.key(),
                record.value() == null ? ByteString.EMPTY : ByteString.copyFrom(record.value()));

        client.eval(
                String.format("return box.space.%s:replace({...})", SPACE_NAME),
                tuple
        ).join();
    }

    public Optional<KVRecord> findByKey(String key) {
        var result = client.eval(
                String.format("return box.space.%s:get({...})", SPACE_NAME),
                List.of(key)
        ).join();

        var tuples = (List<?>) result.get();
        if (tuples.isEmpty() || tuples.get(0) == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(KVMapper.fromTuple((List<?>) tuples.get(0)));
    }

    public boolean deleteByKey(String key) {
        var result = client.eval(
                String.format("return box.space.%s:delete({...})", SPACE_NAME),
                List.of(key)
        ).join();

        var deleted = (List<?>) result.get();
        return !deleted.isEmpty();
    }

    public long countAll() {
        var result = client.eval(
                String.format("return box.space.%s.index.primary:count()", SPACE_NAME),
                Collections.emptyList()
        ).join();

        var counts = (List<?>) result.get();
        return counts.isEmpty() ? 0 : ((Number) counts.get(0)).longValue();
    }

    @SuppressWarnings("unchecked")
    public void streamRange(String keySince, String keyTo, int batchSize, Consumer<KVRecord> consumer) {
        var result = client.eval(
                "local args = {...}; " +
                        "local key_since = args[1]; " +
                        "local key_to = args[2]; " +
                        "local limit = args[3]; " +
                        "local result = {}; " +
                        "for _, t in box.space.KV.index.primary:pairs({key_since}, {iterator = 'GE'}) do " +
                        "  if t[1] >= key_to then break end; " +
                        "  table.insert(result, {t[1], t[2]}); " +
                        "  if #result >= limit then break end; " +
                        "end; " +
                        "return result",
                List.of(keySince, keyTo, batchSize)
        ).join();

        var tuples = (List<?>) result.get();
        for (Object item : tuples) {
            consumer.accept(KVMapper.fromTuple((List<?>) item));
        }
    }
}