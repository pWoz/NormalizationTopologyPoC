package eu.europeana.cloud.processors;

import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.normalization.model.NormalizationResult;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uploads processed file to eCloud.
 */
public class RevisionWritingProcessor implements Processor<String, NormalizationResult, String, NormalizationResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RevisionWritingProcessor.class);

    protected RevisionServiceClient revisionsClient;

    private ProcessorContext<String, NormalizationResult> context;

    @Override
    public void init(ProcessorContext<String, NormalizationResult> context) {
        this.context = context;
        revisionsClient = new RevisionServiceClient(
                context.appConfigs().get("ecloud.url").toString(),
                context.appConfigs().get("ecloud.user").toString(),
                context.appConfigs().get("ecloud.password").toString()
        );
    }

    @Override
    public void process(Record<String, NormalizationResult> processedRecord) {
        LOGGER.info("Writing revision for {}", processedRecord.value());
//        try {
//            revisionsClient.addRevision(
//                    "cloudId",
//                    "repName",
//                    "version",
//                    Revision.fromParams("revName","revProv","tag"));
//        } catch (MCSException e) {
//            throw new RuntimeException(e);
//        }
        context.forward(new Record<>(processedRecord.key(), processedRecord.value(), 10));
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}

