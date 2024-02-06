package cs523.sparkStreaming;

import java.io.BufferedReader;
import java.io.FileReader;
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
import java.util.List;

public class AlphaVantageKafkaProducer {
	public static final String TOPIC="Commodities";
	final static String[] SYMBOLS = {
			"COPPER",
			"NATURAL_GAS",
			"ALUMINUM",
			"WHEAT",
			"CORN",
			"COTTON",
			"SUGAR",
			"COFFEE"};
    final static String[] API_KEYS = {
    		"F0UX5Z8LRINHDYS6",
    		"WPTVWCW0JSQTY5H",
    		"NWQ3EODK9OIF8KFX",
    		"2BQ4M9WFPKL0TOLH",
    		"TI28IVZNOIWYI6GZ",
    		"9D3TLSRVIE9DIDAD",
    		"SEMR2IP1KP893WRB",
    		"7KUZSDJAQ3GX6WCK"
    };
    static String API_KEY = API_KEYS[0];
    static int currentAPI = 0;
	
    public static void main(String[] args) throws Exception {
    	
        //Kafka Properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        
        
        
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        Runnable task = ()->{
        	Producer<String, String> producer= new KafkaProducer<String, String>(props);
        	try {
        		for(String symbol : SYMBOLS ){
        			if(args[0].equals("api"))
        				fetchDataFromAlphaVantage(API_KEY, symbol,producer);
        			else
        				fetchDataFromFile(symbol,producer);
        		}
        			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally{
				producer.close();
			}
        };
        executorService.scheduleAtFixedRate(task, 0, 2, TimeUnit.HOURS);
    }

    private static void fetchDataFromFile(String symbol,Producer<String, String> producer)throws Exception{
    	System.out.println("Reading from the file");
    	String line = "";
    	try(BufferedReader br = new BufferedReader(new FileReader("sample/"+symbol+".csv"))){
    		StringBuilder response = new StringBuilder();
    		while((line=br.readLine())!= null){
    			if(!line.split(",")[1].equals(".")){
            		//kafka msg send
    				//System.out.println(line);
                    //producer.send(new ProducerRecord<String, String>(TOPIC, symbol+","+line));
    				response.append(symbol+","+line+"\n");
            	}
    		}
    		System.out.println(response.toString());
            producer.send(new ProducerRecord<String, String>(TOPIC, response.toString()));
    	}
    	
    }
    
    private static void fetchDataFromAlphaVantage(String apiKey, String symbol,Producer<String, String> producer) throws Exception {
        String apiUrl = "https://www.alphavantage.co/query?function="+symbol+"&interval=daily&apikey="+apiKey+"&datatype=csv";
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        int responseCode = connection.getResponseCode();
        
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
            	
            	System.out.println(inputLine);
            	if(!inputLine.split(",")[1].equals(".")){
            		//kafka msg send
                    response.append(symbol+","+inputLine+"\n");
            	}
            }
            producer.send(new ProducerRecord<String, String>(TOPIC, response.toString()));
            producer.close();
            in.close();
            System.out.println("Sent");
        } else {
            System.out.println("Error fetching data from Alpha Vantage. Response Code: " + responseCode);
            //return null;
        }
    }

}