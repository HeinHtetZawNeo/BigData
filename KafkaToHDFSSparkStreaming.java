import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;

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

        // Create a Kafka input stream with KafkaUtils.createDirectStream
        JavaInputDStream<String> kafkaStream = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        ).map(Tuple2::_2);

        // Inside the foreachRDD block, process JSON data and write to HDFS
        kafkaStream.foreachRDD(rdd -> {
            rdd.foreach(jsonMessage -> {
                try {
                    // Create an HDFS configuration
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://localhost:9000"); // HDFS URI

                    // Initialize the HDFS filesystem
                    FileSystem fs = FileSystem.get(conf);

                    // Define the HDFS path where you want to write the data
                    Path hdfsPath = new Path("/your/hdfs/directory/output.json");

                    // Write the JSON data to HDFS
                    try (FSDataOutputStream outputStream = fs.create(hdfsPath)) {
                        outputStream.writeUTF(jsonMessage);
                    }

                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
