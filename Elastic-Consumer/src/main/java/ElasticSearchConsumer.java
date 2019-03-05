import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ResourceBundle;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {

        ResourceBundle resourceBundle = ResourceBundle.getBundle("elastic-search");

        String hostname = resourceBundle.getString("elastic.host");
        String username = resourceBundle.getString("access.key");
        String password = resourceBundle.getString("access.secret");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;

    }

    public static void main(String[] args) {

        String jsonString = "{\"foo\":\"bar\"}";

        RestHighLevelClient client = createClient();

        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);

        try {
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            String id = indexResponse.getId();
            System.out.println(id);
            client.close();
        }
        catch (ElasticsearchStatusException es) {
            System.out.println(es.status());
        }
        catch (ResponseException re){
            System.out.println(re.getResponse());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {

        }
    }

}
