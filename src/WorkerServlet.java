
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.kevinsawicki.http.HttpRequest;

/**
 * An example Amazon Elastic Beanstalk Worker Tier application. This example
 * requires a Java 7 (or higher) compiler.
 */
public class WorkerServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    
    /**
     * A client to use to access Amazon S3. Pulls credentials from the
     * {@code AwsCredentials.properties} file if found on the classpath,
     * otherwise will attempt to obtain credentials based on the IAM
     * Instance Profile associated with the EC2 instance on which it is
     * run.
     */
    
    /**
     * This method is invoked to handle POST requests from the local
     * SQS daemon when a work item is pulled off of the queue. The
     * body of the request contains the message pulled off the queue.
     */
    @Override
    protected void doPost(final HttpServletRequest request,
                          final HttpServletResponse response)
            throws ServletException, IOException {

        try {

        	int ch;
            StringBuilder sb = new StringBuilder();
            while((ch = request.getInputStream().read())!= -1)
                sb.append((char)ch);
            String m = sb.toString();
            String[] tokens= m.split("::");

			String sentiment = HttpRequest.get("http://access.alchemyapi.com/calls/text/TextGetTextSentiment?apikey=&text=" + tokens[0] + "&outputMode=json").body();
			int start_pos = sentiment.indexOf("type");
			int end_pos = sentiment.indexOf("score");
			String stype = sentiment.substring(start_pos+8, end_pos-4);
			
			AmazonSNS sns = new AmazonSNSClient(new AWSCredentialsProviderChain(
			        new InstanceProfileCredentialsProvider(),
			        new ClasspathPropertiesFileCredentialsProvider()));
			sns.setRegion(Region.getRegion(Regions.US_WEST_2));
			sns.publish("", tokens[5] + "," + tokens[6] + "," + stype);
/*			JSONObject type_obj = (JSONObject)JSONValue.parse(sentiment);
			String type =  type_obj.get("type").toString();
        	final Cluster cluster;
    		final Session session;
    		cluster = Cluster.builder().withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("")).addContactPoint("").build();
    		session = cluster.connect("demo");
    		
            String q = "INSERT INTO twitt_sentiment (keywords, id, content, createddate, date, latitude, longitude, sentiment) VALUES (" + workRequest.getMessage().toString() + ", " + type + ") using TTL 5;";
			session.execute(q);

        */

            response.setStatus(200);

        } catch (RuntimeException exception) {
            
            // Signal to beanstalk that something went wrong while processing
            // the request. The work request will be retried several times in
            // case the failure was transient (eg a temporary network issue
            // when writing to Amazon S3).
            
            response.setStatus(500);
            try (PrintWriter writer =
                 new PrintWriter(response.getOutputStream())) {
                exception.printStackTrace(writer);
            }
        }
    }
    
}
