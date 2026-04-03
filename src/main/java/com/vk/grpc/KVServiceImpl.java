package com.vk.grpc;

import io.grpc.stub.StreamObserver;
import vk.grpc.*;
import vk.grpc.KeyValuePair;
import java.util.Optional;

public class KVServiceImpl extends KeyValueServiceGrpc.KeyValueServiceImplBase {

    private final KVRepository repository;
    public KVServiceImpl(KVRepository repository) {
        this.repository = repository;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutReply> responseObserver) {
        try {
            KVRecord record = new KVRecord(
                    request.getKey(),
                    request.getValue().isEmpty() ? null : request.getValue().toByteArray()
            );

            repository.save(record);

            responseObserver.onNext(PutReply.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            handleGrpcError("put", responseObserver, e);
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetReply> responseObserver) {
        try {
            Optional<KVRecord> recordOpt = repository.findByKey(request.getKey());

            GetReply.Builder builder = GetReply.newBuilder().setSuccess(recordOpt.isPresent());

            if (recordOpt.isPresent()) {
                byte[] value = recordOpt.get().value();
                builder.setValue(KVMapper.toByteString(value));
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            handleGrpcError("get", responseObserver, e);
        }
    }
    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteReply> responseObserver) {
        try {
            boolean deleted = repository.deleteByKey(request.getKey());
            responseObserver.onNext(DeleteReply.newBuilder().setSuccess(deleted).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleGrpcError("delete", responseObserver, e);
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<KeyValuePair> responseObserver) {
        try {
            int batchSize = 1000;

            repository.streamRange(
                    request.getKeySince(),
                    request.getKeyTo(),
                    batchSize,
                    record -> {
                        KeyValuePair pair = KeyValuePair.newBuilder()
                                .setKey(record.key())
                                .setValue(KVMapper.toByteString(record.value()))
                                .build();
                        responseObserver.onNext(pair);
                    }
            );

            responseObserver.onCompleted();
        } catch (Exception e) {
            handleGrpcError("range", responseObserver, e);
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountReply> responseObserver) {
        try {
            long count = repository.countAll();
            responseObserver.onNext(CountReply.newBuilder().setCount((int) count).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            handleGrpcError("count", responseObserver, e);
        }
    }
    private void handleGrpcError(String method, StreamObserver<?> observer, Exception e) {
        System.err.printf("[%s] Error: %s%n", method, e.getMessage());
        e.printStackTrace();

        observer.onError(
                io.grpc.Status.INTERNAL
                        .withDescription(method + " failed: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException()
        );
    }
}