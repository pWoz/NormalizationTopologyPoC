package eu.europeana.cloud.tool;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.dto.Message;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * Verifies if message belongs to the task that was not cancelled;
 * Always return true for taskIds that does not exist;
 */
public class NonCanceledMessages implements Predicate<String, Message> {

    private CassandraTaskInfoDAO dao;

    public NonCanceledMessages(){
        init();
    }

    private void init() {
        dao = new CassandraTaskInfoDAO(
                new CassandraConnectionProvider(
                        "62.3.170.229,62.3.170.241,62.3.170.246,62.3.170.253",
                        9042,
                        "ecloud_dps_v5",
                        "cassandra",
                        "6SYWN054cesP81Rt6ymC")
        );
    }

    @Override
    public boolean test(String key, Message value) {
        try {
            return dao.isDroppedTask(value.getTaskId());
        } catch (TaskInfoDoesNotExistException e) {
            return true;
        }
    }
}
