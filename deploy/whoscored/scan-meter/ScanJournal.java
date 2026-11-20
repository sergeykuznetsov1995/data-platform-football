package football.whoscored;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.ErrorManager;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A JUL handler for the physical reports from the pinned Trino 484 Iceberg connector. */
public final class ScanJournal extends Handler {
    private static final String REPORTER = "io.trino.plugin.iceberg.TrinoMetricsReporter";
    private static final String SCAN = "Received metrics report: ScanReport{tableName=";
    private static final String COMMIT = "Received metrics report: CommitReport{";
    private static final String TIMER = "], scanMetrics=ScanMetricsResult{totalPlanningDuration="
            + "TimerResult{timeUnit=NANOSECONDS, totalDuration=";
    private static final Pattern TABLE = Pattern.compile(
            "(?:bronze|ops)\\.(?:whoscored_[a-z0-9_]+|\"whoscored_[a-z0-9_]+\")");

    private final String generation = UUID.randomUUID().toString();
    private final ArrayBlockingQueue<String> waiting = new ArrayBlockingQueue<>(4096);
    private final AtomicLong errors = new AtomicLong();
    private final Path lease;
    private final FileHandler file;
    private final Thread writer;
    private final ScheduledExecutorService controls;
    private long produced;
    private long processed;
    private long written;
    private long dropped;
    private long invalid;
    private long heartbeats;
    private Boolean lastRequest;
    private volatile boolean enabled;
    private volatile boolean closing;

    public ScanJournal() throws IOException {
        LogManager manager = LogManager.getLogManager();
        String pattern = manager.getProperty(getClass().getName() + ".pattern");
        if (pattern == null || !pattern.startsWith("/") || !pattern.contains("%g")) {
            throw new IOException("absolute rotating scan journal pattern required");
        }
        String control = manager.getProperty(getClass().getName() + ".lease");
        if (control == null || !Path.of(control).isAbsolute()) {
            throw new IOException("absolute activation lease path required");
        }
        lease = Path.of(control);
        file = new FileHandler(pattern, 8 * 1024 * 1024, 16, true);
        file.setLevel(Level.ALL);
        file.setFilter(null);
        file.setEncoding(StandardCharsets.UTF_8.name());
        file.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord row) {
                return row.getMessage() + "\n";
            }
        });
        file.setErrorManager(new ErrorManager() {
            @Override
            public void error(String message, Exception cause, int code) {
                errors.incrementAndGet();
            }
        });
        writer = new Thread(this::drain, "whoscored-scan-journal");
        writer.setDaemon(true);
        controls = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread worker = new Thread(task, "whoscored-scan-health");
            worker.setDaemon(true);
            return worker;
        });
        writer.start();
        controls.scheduleWithFixedDelay(this::checkpoint, 0, 15, TimeUnit.SECONDS);
    }

    @Override
    public void publish(LogRecord record) {
        if (!enabled || !REPORTER.equals(record.getLoggerName()) || !Level.FINE.equals(record.getLevel())) {
            return;
        }
        String message = record.getMessage();
        try {
            if (message.startsWith(COMMIT)) {
                return;
            }
            if (!message.startsWith(SCAN)) {
                throw new IllegalArgumentException();
            }
            int end = message.indexOf(", snapshotId=", SCAN.length());
            if (end < 0) {
                throw new IllegalArgumentException();
            }
            String name = message.substring(SCAN.length(), end);
            Matcher target = TABLE.matcher(name);
            if (!target.matches()) {
                if (name.contains("whoscored_")) {
                    throw new IllegalArgumentException();
                }
                return;
            }
            int snapStart = end + ", snapshotId=".length();
            long snapshot = Long.parseLong(message, snapStart, message.indexOf(',', snapStart), 10);
            int timer = message.lastIndexOf(TIMER);
            if (timer < snapStart) {
                throw new IllegalArgumentException();
            }
            int start = timer + TIMER.length();
            long nanos = Duration.parse(message.substring(start, message.indexOf(',', start))).toNanos();
            if (snapshot < 0 || nanos < 0) {
                throw new IllegalArgumentException();
            }
            enqueue(record.getInstant(), name.replace("\"", ""), snapshot, nanos);
        }
        catch (RuntimeException e) {
            synchronized (this) {
                invalid++;
            }
        }
    }

    private synchronized void enqueue(Instant at, String table, long snapshot, long nanos) {
        long seq = ++produced;
        String line = String.join("\t", "SCAN1", generation, Long.toString(seq), at.toString(),
                table, Long.toString(snapshot), Long.toString(nanos));
        if (closing || !waiting.offer(line)) {
            dropped++;
        }
    }

    private void drain() {
        try {
            while (!closing || !waiting.isEmpty()) {
                String line = waiting.poll(1, TimeUnit.SECONDS);
                if (line != null) {
                    long before = errors.get();
                    file.publish(new LogRecord(Level.INFO, line));
                    synchronized (this) {
                        processed++;
                        if (errors.get() == before) {
                            written++;
                        }
                    }
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            errors.incrementAndGet();
        }
        catch (RuntimeException e) {
            errors.incrementAndGet();
        }
        finally {
            file.close();
        }
    }

    private boolean leaseIsCurrent() {
        try {
            if (!Files.isRegularFile(lease) || Files.size(lease) > 64) {
                return false;
            }
            return Instant.parse(Files.readString(lease, StandardCharsets.US_ASCII).strip()).isAfter(Instant.now());
        }
        catch (IOException | RuntimeException e) {
            return false;
        }
    }

    private void checkpoint() {
        Logger reporter = Logger.getLogger(REPORTER);
        boolean installed = false;
        for (Handler handler : reporter.getHandlers()) {
            installed |= handler == this;
        }
        boolean requested = !closing && installed && writer.isAlive() && leaseIsCurrent();
        if (lastRequest == null || requested != lastRequest) {
            reporter.setLevel(requested ? Level.FINE : null);
            lastRequest = requested;
        }
        enabled = requested && reporter.isLoggable(Level.FINE);
        String line;
        synchronized (this) {
            line = String.join("\t", "WS_SCAN_HEALTH1", generation, Instant.now().toString(),
                    Long.toString(++heartbeats),
                    Long.toString(produced), Long.toString(written), Long.toString(dropped),
                    Long.toString(invalid), Long.toString(errors.get()), Long.toString(produced - dropped - processed),
                    Boolean.toString(enabled));
        }
        // This logger goes to the ordinary coordinator console, independent of the rotating file.
        Logger.getLogger("football.whoscored.scan.health").info(line);
    }

    @Override
    public void flush() {
        // FileHandler flushes each dequeued record. Request threads never wait for disk.
    }

    @Override
    public void close() {
        enabled = false;
        closing = true;
        controls.shutdownNow();
        if (Thread.currentThread() != writer) {
            try {
                writer.join(2500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
