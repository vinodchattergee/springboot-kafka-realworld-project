package net.vinlabs.springboot;

import net.vinlabs.springboot.entity.WikimediaData;
import net.vinlabs.springboot.repository.WikimediaDataRepository;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    public KafkaDatabaseConsumer(WikimediaDataRepository repository) {
        this.repository = repository;
    }

    private WikimediaDataRepository repository;

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String eventMessage) {
        LOGGER.info(String.format("Message received -> %s",eventMessage));
        WikimediaData wikimediaData=new WikimediaData();
        wikimediaData.setEventData(eventMessage);

        repository.save(wikimediaData);

    }
}
