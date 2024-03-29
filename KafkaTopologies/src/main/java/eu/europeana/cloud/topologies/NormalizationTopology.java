package eu.europeana.cloud.topologies;

import eu.europeana.cloud.dto.MessageSerde;
import eu.europeana.cloud.processors.*;
import eu.europeana.cloud.tool.NonCanceledMessages;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class NormalizationTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationTopology.class);

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "normalization-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put("ecloud.url", "");
        props.put("ecloud.user", "");
        props.put("ecloud.password", "");

        return props;
    }

    private static Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("files-for-normalization", Consumed.with(Serdes.String(), new MessageSerde()))
                .filter(new NonCanceledMessages())
                .process(FileDecompressingProcessor::new)
                .process(NormalizationProcessor::new)
                .process(FileUploadingProcessor::new)
                .foreach((key, value) ->
                        LOGGER.info("File processed"));
        return builder.build();
    }
}