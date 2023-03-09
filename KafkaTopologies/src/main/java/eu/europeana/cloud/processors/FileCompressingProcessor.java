package eu.europeana.cloud.processors;

import eu.europeana.cloud.dto.Message;
import eu.europeana.cloud.tool.FileCompressor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;

/**
 * Compresses the file;
 */
public class FileCompressingProcessor implements Processor<String, Message, String, Message> {

    private ProcessorContext<String, Message> context;
    private FileCompressor fileCompressor;

    @Override
    public void init(ProcessorContext<String, Message> context) {
        this.context = context;
        fileCompressor = new FileCompressor();
    }

    @Override
    public void process(Record<String, Message> record) {
        byte[] compressedFileContent = fileCompressor.compress(record.value().getPayload().getBytes(StandardCharsets.UTF_8));
        Message output = Message
                .builder()
                .payload(new String(compressedFileContent))
                .build();
        context.forward(new Record<>(output.getMessageId(), output, 10));
    }
}
