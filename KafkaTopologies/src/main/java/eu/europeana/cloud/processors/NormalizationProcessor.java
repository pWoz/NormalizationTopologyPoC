package eu.europeana.cloud.processors;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Does the normalization for the file
 */
public class NormalizationProcessor implements Processor<String, String, String, NormalizationResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationProcessor.class);
    private Normalizer normalizer;

    private ProcessorContext<String, NormalizationResult> context;

    private FileServiceClient fileClient;

    @Override
    public void init(ProcessorContext<String, NormalizationResult> context) {
        this.context = context;
        try {
            NormalizerFactory normalizerFactory = new NormalizerFactory();
            normalizer = normalizerFactory.getNormalizer();
            fileClient = new FileServiceClient(
                    context.appConfigs().get("ecloud.url").toString(),
                    context.appConfigs().get("ecloud.user").toString(),
                    context.appConfigs().get("ecloud.password").toString()
            );
        } catch (NormalizationConfigurationException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void process(Record<String, String> record) {
        String document = null;
        try {
            String fileUrl = record.value();
            InputStream file = fileClient.getFile(fileUrl);
            document = new String(file.readAllBytes(), StandardCharsets.UTF_8);
            NormalizationResult normalizationResult = normalizer.normalize(document);
            LOGGER.info("File normalized: {}", normalizationResult.getNormalizedRecordInEdmXml());
            context.forward(new Record<>(record.key(), normalizationResult, 10));
        } catch (MCSException | NormalizationException | IOException e) {
            LOGGER.error("Error while normalizing file {}", record.value());
            context.forward(new Record<>(record.key(), NormalizationResult.createInstanceForError(e.getMessage(), record.value()), 10));
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
