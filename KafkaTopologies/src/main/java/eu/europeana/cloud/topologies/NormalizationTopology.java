package eu.europeana.cloud.topologies;

import eu.europeana.cloud.processors.*;
import eu.europeana.normalization.model.NormalizationResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class NormalizationTopology {

    public static void main(String[] args) {
        Properties props = prepareProps();
        final Topology topology = buildTopology();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties prepareProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put("ecloud.url", "<url>");
        props.put("ecloud.user", "<user>");
        props.put("ecloud.password", "<password>");

        return props;
    }

    private static Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        Map<String, KStream<String, NormalizationResult>> branches = builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .process(NormalizationProcessor::new)
                .split(Named.as("branch-"))
                .branch((key, value) -> value.getErrorMessage() != null, Branched.as("errors"))
                .defaultBranch(Branched.as("default"));

        branches.get("branch-errors")
                .process(NormalizationErrorProcessor::new);
        branches.get("branch-default")
                .process(FileUploadProcessor::new)
                .process(RevisionWritingProcessor::new)
                .process(NotificationProcessor::new);

        return builder.build();
    }
}