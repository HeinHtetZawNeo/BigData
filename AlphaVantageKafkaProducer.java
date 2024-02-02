import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.LinkedBlockingQueue;

public class AlphaVantageKafkaProducer {
    public static void main(String[] args) throws Exception {
        final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(1000);
        String alphaVantageApiKey = "YOUR_ALPHA_VANTAGE_API_KEY";
        String symbol = "IBM";
        String interval = "5min";
        int j = 0;
        while (true) {
            String alphaVantageData = fetchDataFromAlphaVantage(alphaVantageApiKey, symbol, interval);
            if (alphaVantageData == null) {
                Thread.sleep(100);
            } else {
                System.out.println("Alpha Vantage Data: " + alphaVantageData);
            }
        }
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