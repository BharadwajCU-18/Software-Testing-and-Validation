import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Simple event-driven task scheduler.
 * - Producers publish Event objects.
 * - Consumers register handlers per event type.
 * - Handlers run asynchronously on a fixed thread pool.
 */
public final class EventDrivenTaskScheduler implements AutoCloseable {
    public static final class Event {
        private final String type;
        private final String id;
        private final Instant createdAt;
        private final Map<String, Object> payload;

        public Event(String type, Map<String, Object> payload) {
            this.type = Objects.requireNonNull(type, "type");
            this.id = UUID.randomUUID().toString();
            this.createdAt = Instant.now();
            this.payload = payload == null ? Map.of() : Map.copyOf(payload);
        }

        public String getType() { return type; }
        public String getId() { return id; }
        public Instant getCreatedAt() { return createdAt; }
        public Map<String, Object> getPayload() { return payload; }
    }

    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private final Map<String, List<Consumer<Event>>> handlers = new ConcurrentHashMap<>();
    private final ExecutorService workerPool;
    private final Thread dispatcherThread;
    private volatile boolean running = true;

    public EventDrivenTaskScheduler(int workers) {
        if (workers <= 0) throw new IllegalArgumentException("workers must be > 0");
        this.workerPool = Executors.newFixedThreadPool(workers);
        this.dispatcherThread = new Thread(this::dispatchLoop, "event-dispatcher");
        this.dispatcherThread.setDaemon(true);
        this.dispatcherThread.start();
    }

    public void registerHandler(String eventType, Consumer<Event> handler) {
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(handler, "handler");
        handlers.computeIfAbsent(eventType, k -> new ArrayList<>()).add(handler);
    }

    public void publish(Event event) {
        Objects.requireNonNull(event, "event");
        if (!running) throw new IllegalStateException("scheduler is closed");
        queue.offer(event);
    }

    private void dispatchLoop() {
        while (running) {
            try {
                Event event = queue.take();
                List<Consumer<Event>> eventHandlers = handlers.get(event.getType());
                if (eventHandlers != null) {
                    for (Consumer<Event> handler : eventHandlers) {
                        workerPool.submit(() -> handler.accept(event));
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        running = false;
        dispatcherThread.interrupt();
        workerPool.shutdown();
        try {
            workerPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            workerPool.shutdownNow();
        }
    }

    public static void main(String[] args) {
        EventDrivenTaskScheduler scheduler = new EventDrivenTaskScheduler(4);
        scheduler.registerHandler("EMAIL", e ->
            System.out.println("Send email for event " + e.getId() + " at " + e.getCreatedAt())
        );
        scheduler.registerHandler("REPORT", e ->
            System.out.println("Generate report with payload " + e.getPayload())
        );

        scheduler.publish(new Event("EMAIL", Map.of("to", "user@example.com")));
        scheduler.publish(new Event("REPORT", Map.of("range", "last-24h")));

        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
        scheduler.close();
    }
}
