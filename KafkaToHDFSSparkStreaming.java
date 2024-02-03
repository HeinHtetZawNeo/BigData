import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class KafkaToHDFSSparkStreaming {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaToHDFSSparkStreaming")
                .setMaster("local[*]"); // Use your Spark master URL

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // Kafka parameters
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092"); // Kafka broker(s)

        Set<String> topics = new HashSet<>();
        topics.add("your_kafka_topic"); // Replace with your Kafka topic

        // Create a Kafka input stream as JavaPairInputDStream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        // Extract and process JSON messages from Kafka stream
        kafkaStream.foreachRDD(rdd -> {
            rdd.foreach(tuple -> {
                String key = tuple._1();
                String jsonMessage = tuple._2();

                // Inside this block, you can process the JSON messages as needed
                System.out.println("Received JSON message: " + jsonMessage);

                // Write the JSON message to HDFS
                writeToHDFS(jsonMessage);
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    // Function to write data to HDFS
    private static void writeToHDFS(String data) {
        try {
            // Create an HDFS configuration
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000"); // HDFS URI

            // Initialize the HDFS filesystem
            FileSystem fs = FileSystem.get(conf);

            // Define the HDFS path where you want to write the data
            Path hdfsPath = new Path("/your/hdfs/directory/output.json");

            // Write the data to HDFS
            try (FSDataOutputStream outputStream = fs.create(hdfsPath)) {
                outputStream.writeUTF(data);
            }

            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
