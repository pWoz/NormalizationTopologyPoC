package eu.europeana.cloud.processors;

import eu.europeana.cloud.dto.Message;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

/**
 * Downloads file from eCloud.
 */
public class FileUploadingProcessor implements Processor<String, Message, String, Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadingProcessor.class);

    private FileServiceClient fileClient;

    private ProcessorContext<String, Message> context;

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;

        fileClient = new FileServiceClient(
                context.appConfigs().get("ecloud.url").toString(),
                context.appConfigs().get("ecloud.user").toString(),
                context.appConfigs().get("ecloud.password").toString()
        );
    }

    @Override
    public void process(Record<String, Message> processedRecord) {
        try {
            LOGGER.info("The following file will be uploaded: {}", processedRecord.value().getPayload());

            //TODO proper values has to be used here;
            fileClient.uploadFile("cid","repname","version",
                    new ByteArrayInputStream(processedRecord.value().getPayload().getBytes()),"application/xml");
            LOGGER.info("File uploaded to eCloud");
            context.forward(new Record<>(processedRecord.value().getMessageId(), processedRecord.value(), 10));

        } catch (MCSException e) {
            LOGGER.error("Unable to upload the following file {}",processedRecord.value().getPayload());
            LOGGER.error("Stopping processing for file {}",processedRecord.value().getPayload());
        }

    }

    @Override
    public void close() {
        Processor.super.close();
    }

}

