import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    public void run() {

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client = createTwitterClient(msgQueue);

        client.connect();

        while(!client.isDone()){
            String message = null;

            try {
                message = msgQueue.poll(5, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                client.stop();
            }

            if(message != null) {
                System.out.println(message);
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        ResourceBundle resourceBundle = ResourceBundle.getBundle("twitter");

        String consumerKey = resourceBundle.getString("api.key");
        String consumerSecret = resourceBundle.getString("api.key.secret");
        String accessToken = resourceBundle.getString("access.token");
        String accessTokenSecret = resourceBundle.getString("access.token.secret");

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("iaf");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Kafka-Twitter-Client")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

}
