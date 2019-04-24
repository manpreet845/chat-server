package server;

import manicorp.Proto;
import server.kafka.KafkaChatConsumer;
import server.kafka.KafkaChatProducer;

public class Application {
  public static void main(String[] args) {
    KafkaChatProducer producer = new KafkaChatProducer();
    KafkaChatConsumer consumer = new KafkaChatConsumer();

    try {
      for (int i = 0; i < 10; i++) {
        long time = System.currentTimeMillis();
        byte[] message = Proto.Message.newBuilder().setMessage("chat").build().toByteArray();
        producer.ProduceMessage("Message", time, message);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      consumer.ConsumeMessage();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    System.out.println("hello");
  }
}
