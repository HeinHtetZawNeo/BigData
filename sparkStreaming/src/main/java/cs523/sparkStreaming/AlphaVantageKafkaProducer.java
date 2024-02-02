package cs523.sparkStreaming;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AlphaVantageKafkaProducer {
    public static void main(String[] args) throws Exception {
        //String alphaVantageApiKey = "WPTVWCWOJSQTY5H";
    	String alphaVantageApiKey = "demo";
        String symbol = "IBM";
        String interval = "5min";
        String topic="alphaV";
        
        //Kafka Properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        
        
        
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        Runnable task = ()->{
        	Producer<String, String> producer= new KafkaProducer<String, String>(props);
        	try {
				String alphaVantageData = fetchDataFromAlphaVantage(alphaVantageApiKey, symbol, interval);
				System.out.println("I am here");
            	producer.send(new ProducerRecord<String, String>(topic, alphaVantageData));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally{
				producer.close();
			}
        };
        
        executorService.scheduleAtFixedRate(task, 0, 10, TimeUnit.SECONDS);
    }

    private static String fetchDataFromAlphaVantage(String apiKey, String symbol, String interval) throws Exception {
        String apiUrl = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol="
                + symbol + "&interval=" + interval + "&outputsize=full&apikey=" + apiKey;
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            return response.toString();
        } else {
            System.out.println("Error fetching data from Alpha Vantage. Response Code: " + responseCode);
            return null;
        }
    }

}