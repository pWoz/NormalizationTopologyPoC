package eu.europeana.cloud.dto;

import lombok.Builder;
import lombok.Data;

/**
 * Message that is sent vi Kafka topics
 */
@Data
@Builder
public class Message {
    private long taskId;
    private String messageId;
    private String payload;
}
