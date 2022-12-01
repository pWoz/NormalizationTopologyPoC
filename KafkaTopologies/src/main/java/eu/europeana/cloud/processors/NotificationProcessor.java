package eu.europeana.cloud.processors;

import eu.europeana.normalization.model.NormalizationResult;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uploads processed file to eCloud.
 */
public class NotificationProcessor implements Processor<String, NormalizationResult, String, NormalizationResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationProcessor.class);

    private ProcessorContext<String, NormalizationResult> context;

    @Override
    public void init(ProcessorContext<String, NormalizationResult> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, NormalizationResult> processedRecord) {
        LOGGER.info("Processing notification for {}", processedRecord.value());
        context.forward(new Record<>(processedRecord.key(), processedRecord.value(), 10));
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}

