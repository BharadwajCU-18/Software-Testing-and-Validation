package coen448.computablefuture.test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class AsyncProcessor {
    public CompletableFuture<String> processAsync(List<Microservice> microservices) {
        if (microservices == null || microservices.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<String>> futures = microservices.stream()
            .map(client -> client.retrieveAsync("hello"))
            .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .limit(1)
                .collect(Collectors.joining(" ")));
    }
}
