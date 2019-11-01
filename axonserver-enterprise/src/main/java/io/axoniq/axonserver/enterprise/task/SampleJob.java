package io.axoniq.axonserver.enterprise.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Marc Gathier
 */
@RestController
public class SampleJob implements ScheduledJob {

    private final Logger logger = LoggerFactory.getLogger(SampleJob.class);
    private final TaskPublisher taskPublisher;

    public SampleJob(TaskPublisher taskPublisher) {
        this.taskPublisher = taskPublisher;
    }

    @GetMapping(path = "/v1/schedule")
    public void schedule(@RequestParam(name = "delay", required = false, defaultValue = "500") long delay)
            throws JsonProcessingException {
        SamplePayload samplePayload = new SamplePayload();
        samplePayload.setData("This is the data");
        taskPublisher.publishTask(getClass().getName(), samplePayload, delay);
    }


    @Override
    public void execute(Object payload) {
        SamplePayload samplePayload = (SamplePayload) payload;
        logger.warn("Executing: {}", samplePayload.getData());
    }


    public static class SamplePayload {

        private String data;

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }
    }
}
