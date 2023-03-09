package eu.europeana.cloud.processors;

import eu.europeana.cloud.dto.Message;
import eu.europeana.cloud.tool.FileCompressor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.DataFormatException;

public class FileDecompressingProcessor implements Processor<String, Message, String, Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileDecompressingProcessor.class);

    private ProcessorContext<String, Message> context;
    private FileCompressor fileCompressor;

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;
        fileCompressor = new FileCompressor();
    }

    @Override
    public void process(Record<String, Message> record) {
        byte[] compressedFileContent = new byte[0];
        try {
            compressedFileContent = fileCompressor.decompress(record.value().getPayload().getBytes(StandardCharsets.UTF_8));
            Message output = Message
                    .builder()
                    .payload(new String(compressedFileContent))
                    .build();
            context.forward(new Record<>(output.getMessageId(), output, 10));
        } catch (IOException | DataFormatException e) {
            LOGGER.error("Decompressing failed. File processing stopped");
        }
    }
}
