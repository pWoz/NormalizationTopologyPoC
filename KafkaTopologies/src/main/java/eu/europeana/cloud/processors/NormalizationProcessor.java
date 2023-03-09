package eu.europeana.cloud.processors;

import eu.europeana.cloud.dto.Message;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.model.NormalizationResult;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import eu.europeana.normalization.util.NormalizationException;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Does the normalization for the file
 */
public class NormalizationProcessor implements Processor<String, Message, String, Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationProcessor.class);
    private Normalizer normalizer;

    private ProcessorContext<String, Message> context;

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;
        try {
            NormalizerFactory normalizerFactory = new NormalizerFactory();
            normalizer = normalizerFactory.getNormalizer();
        } catch (NormalizationConfigurationException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void process(Record<String, Message> record) {
        String document = record.value().getPayload();
        try {
            LOGGER.info("The following file will be processed: {}", document);
            NormalizationResult normalizationResult = normalizer.normalize(document);
            LOGGER.info("File normalized: {}", normalizationResult.getNormalizedRecordInEdmXml());
            context.forward(new Record<>(
                    record.key(),
                    Message.builder()
                            .payload(normalizationResult.getNormalizedRecordInEdmXml())
                            .build(), 10));
        } catch (NormalizationException e) {
            LOGGER.error("Error while normalizing file {}", record.value());
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
