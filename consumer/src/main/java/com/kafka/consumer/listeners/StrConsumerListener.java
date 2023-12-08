package com.kafka.consumer.listeners;

import com.kafka.consumer.custom.StrConsumerCustomListener;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class StrConsumerListener {

    @SneakyThrows
    @StrConsumerCustomListener(groupId = "group-1")
    public void create(String message){
        log.info("CREATE ::: Received message: {}", message);
    }

    @StrConsumerCustomListener(groupId = "group-1")
    public void log(String message){
        log.info("LOG ::: Received message: {}", message);
        throw new IllegalArgumentException("EXCEPTION ...");

    }

    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "validMessageContainerFactory")
    public void history(String message){
        log.info("HISTORY ::: Received message: {}", message);
    }
}
