package cs523.kafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String topic="alphaV";
        Properties props = new Properties();
        
        props.put("bootstrap.servers", "192.168.238.128:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        
        for(int i=0;i<10;i++){
        	producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i),"Message "+i+"testing123"));
        }
        
        producer.close();
    }
}
