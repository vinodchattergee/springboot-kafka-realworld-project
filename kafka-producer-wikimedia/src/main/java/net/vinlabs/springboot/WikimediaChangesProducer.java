package net.vinlabs.springboot;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {
    private static final Logger LOGGER= LoggerFactory.getLogger(WikimediaChangesProducer.class);
    @Value("${spring.kafka.topic.name}")
    private String topicName;

    private KafkaTemplate<String,String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {
        // To read a realtime stream data from wikimedia , we use event source

        EventHandler eventHandler=new WikimediaChangesHandler(kafkaTemplate,topicName);
        String uri="https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder=new EventSource.Builder(eventHandler, URI.create(uri));
        EventSource eventSource=builder.build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);

    }
}
