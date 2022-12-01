package eu.europeana.cloud.processors;

import eu.europeana.normalization.model.NormalizationResult;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes errors from normalization step
 */
public class NormalizationErrorProcessor implements Processor<String, NormalizationResult, String, NormalizationResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationErrorProcessor.class);

    private ProcessorContext<String, NormalizationResult> context;

    @Override
    public void init(ProcessorContext<String, NormalizationResult> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, NormalizationResult> record) {
        LOGGER.info("Processing normalization error {}", record.value());
        //store information about error in notifications
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
