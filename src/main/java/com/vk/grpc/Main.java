package com.vk.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolBoxClientBuilder;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;

import java.util.Collections;

public class Main {
    public static void main(String[] args) throws Exception {
        InstanceConnectionGroup group = InstanceConnectionGroup.builder()
                .withHost("localhost")
                .withPort(3301)
                .build();

        TarantoolBoxClientBuilder builder = TarantoolFactory.box()
                .withGroups(Collections.singletonList(group));

        try (TarantoolBoxClient client = builder.build()) {
            client.eval("return 'OK'", String.class).join();
            System.out.println("Tarantool connected");

            KVRepository repository = new KVRepository(client);
            KVServiceImpl service = new KVServiceImpl(repository);

            Server server = ServerBuilder.forPort(9090)
                    .addService(service)
                    .build()
                    .start();

            System.out.println("gRPC Server started on port 9090");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down server...");
                server.shutdown();
            }));

            server.awaitTermination();
        }
    }
}