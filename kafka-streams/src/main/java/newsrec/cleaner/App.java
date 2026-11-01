package newsrec.cleaner;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Edge cleaning layer (report Section 5.1 "Ingestion"):
 * articles.raw / events.raw -> validate (JSON Schema) -> dedup -> enrich -> *.clean, failures -> dlq.
 */
public final class App {
    static final String ARTICLE_META_STORE = "article-meta";
    private static final String DEDUP_ARTICLES = "dedup-articles";
    private static final String DEDUP_EVENTS = "dedup-events";

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    static Topology topology(String articlesRaw, String eventsRaw, String articlesClean,
                             String eventsClean, String dlq, Duration dedupWindow) {
        StreamsBuilder b = new StreamsBuilder();
        // In-memory stores (changelog-backed) avoid RocksDB's off-heap footprint in a 256 MB container.
        b.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(DEDUP_ARTICLES), Serdes.String(), Serdes.Long()));
        b.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(DEDUP_EVENTS), Serdes.String(), Serdes.Long()));
        b.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(ARTICLE_META_STORE), Serdes.String(), Serdes.String()));

        Consumed<String, String> in = Consumed.with(Serdes.String(), Serdes.String());
        Produced<String, String> out = Produced.with(Serdes.String(), Serdes.String());
        SchemaValidator articleSchema = new SchemaValidator("/schemas/article.schema.json");
        SchemaValidator eventSchema = new SchemaValidator("/schemas/event.schema.json");

        KStream<String, Checked> articles = b.stream(articlesRaw, in).process(
                () -> new CleaningProcessor(CleaningProcessor.Kind.ARTICLE, articleSchema, DEDUP_ARTICLES, dedupWindow),
                DEDUP_ARTICLES, ARTICLE_META_STORE);
        KStream<String, Checked> events = b.stream(eventsRaw, in).process(
                () -> new CleaningProcessor(CleaningProcessor.Kind.EVENT, eventSchema, DEDUP_EVENTS, dedupWindow),
                DEDUP_EVENTS, ARTICLE_META_STORE);

        articles.filter((k, c) -> c.isOk()).mapValues(Checked::json).to(articlesClean, out);
        articles.filter((k, c) -> !c.isOk()).mapValues(c -> c.toDlq(articlesRaw)).to(dlq, out);
        events.filter((k, c) -> c.isOk()).mapValues(Checked::json).to(eventsClean, out);
        events.filter((k, c) -> !c.isOk()).mapValues(c -> c.toDlq(eventsRaw)).to(dlq, out);
        return b.build();
    }

    public static void main(String[] args) throws InterruptedException {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "newsrec-cleaner");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP", "kafka:9092"));
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);  // forward immediately: latency over batching
        // Wait briefly for both inputs so records are processed in timestamp order across topics
        // (an article is produced before its first event, so it reaches the meta store first).
        p.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 200);

        Topology t = topology(
                env("TOPIC_ARTICLES_RAW", "articles.raw"), env("TOPIC_EVENTS_RAW", "events.raw"),
                env("TOPIC_ARTICLES_CLEAN", "articles.clean"), env("TOPIC_EVENTS_CLEAN", "events.clean"),
                env("TOPIC_DLQ", "dlq"), Duration.ofMinutes(Long.parseLong(env("DEDUP_WINDOW_MIN", "10"))));
        System.out.println(t.describe());

        KafkaStreams streams = new KafkaStreams(t, p);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { streams.close(Duration.ofSeconds(10)); latch.countDown(); }));
        streams.start();
        latch.await();
    }
}
