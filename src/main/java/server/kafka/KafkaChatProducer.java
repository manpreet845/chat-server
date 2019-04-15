package server.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaChatProducer {

    private final static String TOPIC = "Message";
    private final static String BOOTSTRAP_SERVERS =
        "localhost:9092";

  private Producer<Long, chatserver.Proto.Message> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        LongSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  public void ProduceMessage(final String message) throws Exception {
    final Producer<Long, chatserver.Proto.Message> producer = createProducer();
    long time = System.currentTimeMillis();

    try {
        final ProducerRecord<Long, chatserver.Proto.Message> record =
            new ProducerRecord<>(TOPIC, 0l,
                    chatserver.Proto.Message.newBuilder().setMessage(message).build());

        RecordMetadata metadata = producer.send(record).get();

        System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
            record.key(), record.value(), metadata.partition(),
            metadata.offset());

    } finally {
      producer.flush();
      producer.close();
    }
  }

}
