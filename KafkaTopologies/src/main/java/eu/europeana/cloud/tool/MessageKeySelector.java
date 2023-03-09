package eu.europeana.cloud.tool;

import eu.europeana.cloud.dto.Message;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class MessageKeySelector implements KeyValueMapper<String, Message, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageKeySelector.class);

    private static final int BUCKETS = 2;

    @Override
    public String apply(String key, Message value) {
        int random = new Random().nextInt(BUCKETS);
        String partitionKey = value.getTaskId() + "_" + random;
        LOGGER.info("The following partition key was selected: {}", partitionKey);
        return partitionKey;
    }
}
