package server.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import manicorp.Proto;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import java.util.Collections;
import java.util.Properties;

public class KafkaChatConsumer {
  private final static String TOPIC = "Message";
  private final static String BOOTSTRAP_SERVERS =
      "localhost:9092";

  private Consumer<Long, byte[]> createConsumer() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
        "KafkaExampleConsumer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        LongDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName());

    // Create the consumer using props.
    final Consumer<Long, byte[]> consumer =
        new KafkaConsumer<>(props);

    // Subscribe to the topic.
    consumer.subscribe(Collections.singletonList(TOPIC));
    return consumer;
  }


  public void ConsumeMessage() throws InterruptedException {
    final Consumer<Long, byte[]> consumer = createConsumer();

    int giveUp = 100;
    int noRecordsCount = 0;

    while (true) {
      final ConsumerRecords<Long, byte[]> consumerRecords =
          consumer.poll(1000);

      if (consumerRecords.count()==0) {
        noRecordsCount++;
        if (noRecordsCount > giveUp) break;
        else continue;
      }

      consumerRecords.forEach(record -> {
        try {
          System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
              record.key(),
                  Proto.Message.parseFrom(record.value()),
                  record.partition(),
                  record.offset());
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }
      });

      consumer.commitAsync();
    }
    consumer.close();
    System.out.println("DONE");
  }
}
