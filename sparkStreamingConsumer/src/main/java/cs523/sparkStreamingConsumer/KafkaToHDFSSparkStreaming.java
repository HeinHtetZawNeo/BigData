package cs523.sparkStreamingConsumer;

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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class KafkaToHDFSSparkStreaming {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaToHDFSSparkStreaming")
                .set("spark.hadoop.version","2.6.0-cdh5.13.0")
                .setMaster("local[*]"); // Use your Spark master URL

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        // Kafka parameters
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092"); // Kafka broker(s)

        Set<String> topics = new HashSet<>();
        topics.add("Commodities"); // Kafka topic

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
        System.out.println("Before Foreach");
        // Extract and process JSON messages from Kafka stream
        kafkaStream.foreachRDD(rdd -> {
            rdd.foreach(tuple -> {
                String key = tuple._1();
                String value = tuple._2();
                System.out.println("in the kafkaStream with key - "+key);
                // Inside this block, you can process the JSON messages as needed
                //System.out.println("Received value: " + value);

                // Write the JSON message to HDFS
                writeToHDFS(value,value.split(",")[0]);
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    	//writeToHDFS("Test Data");
    }

    // Function to write data to HDFS
    private static void writeToHDFS(String data,String type) {
    	
        try {
            // Create an HDFS configuration
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020"); // HDFS URI

            // Initialize the HDFS filesystem
            FileSystem fs = FileSystem.get(conf);
            
            // Define the HDFS path where you want to write the data
            Path hdfsPath = new Path("/user/cloudera/project/"+LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy"))+"_"+type+".csv");

            // Write the data to HDFS
            FSDataOutputStream outputStream = null;
        	
            if(fs.exists(hdfsPath)){
            	System.out.println("Exist");
            	outputStream = fs.append(hdfsPath);
            }else{
            	System.out.println("Create");
            	outputStream = fs.create(hdfsPath);
            }
            outputStream.writeUTF(data);
            //BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream,"UTF-8"));
            //writer.write(data);
            //writer.newLine();
            //writer.close();
            outputStream.close();
        	fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally{
        	
        }
    }
}
