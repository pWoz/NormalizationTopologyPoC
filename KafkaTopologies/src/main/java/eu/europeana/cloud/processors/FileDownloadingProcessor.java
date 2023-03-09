package eu.europeana.cloud.processors;

import eu.europeana.cloud.dto.Message;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Downloads file from eCloud.
 */
public class FileDownloadingProcessor implements Processor<String, Message, String, Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileDownloadingProcessor.class);

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
        LOGGER.info("Downloading the following file {}", processedRecord.value());
        try {
            InputStream file = fileClient.getFile(processedRecord.value().getPayload());
            String fileContent = new String(file.readAllBytes());
            LOGGER.info("Downloaded the following file {}", fileContent);
            Message output = Message
                    .builder()
                    .payload(fileContent)
                    .build();
            context.forward(new Record<>(output.getMessageId(), output, 10));

        } catch (MCSException | IOException e) {
            LOGGER.error("Unable to download the following file {}", processedRecord.value().getPayload());
            LOGGER.error("Stopping processing for file {}", processedRecord.value().getPayload());
        }

    }

    @Override
    public void close() {
        Processor.super.close();
    }

}

