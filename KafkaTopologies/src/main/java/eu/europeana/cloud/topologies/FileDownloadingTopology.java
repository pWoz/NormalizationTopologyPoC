package eu.europeana.cloud.topologies;

import eu.europeana.cloud.dto.MessageSerde;
import eu.europeana.cloud.processors.FileCompressingProcessor;
import eu.europeana.cloud.processors.FileDownloadingProcessor;
import eu.europeana.cloud.tool.MessageKeySelector;
import eu.europeana.cloud.tool.NonCanceledMessages;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams topology responsible for downloading xml files from eCloud.
 * This is substitute for ReadFileBolt in our current topologies;
 */
public class FileDownloadingTopology {

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

        props.put("ecloud.url", "");
        props.put("ecloud.user", "");
        props.put("ecloud.password", "");

        return props;
    }

    private static Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("files-for-downloading", Consumed.with(Serdes.String(), new MessageSerde()))
                .filter(new NonCanceledMessages())
                .process(FileDownloadingProcessor::new)
                .process(FileCompressingProcessor::new, Named.as("file_compressor"))
                .selectKey(new MessageKeySelector())
                .to("files-for-normalization", Produced.with(Serdes.String(), new MessageSerde()));
        return builder.build();
    }
}