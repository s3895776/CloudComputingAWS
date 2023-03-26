package DatabaseManagement;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PopulateMusicTable {

    public static void main(String[] args) throws Exception {
    	
    	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            	.withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider("default"))
                .build();

            DynamoDB dynamoDB = new DynamoDB(client);

            Table table = dynamoDB.getTable("music");
           
            JsonParser parser = new JsonFactory().createParser(new File("a1.json"));
            
            JsonNode rootNode = new ObjectMapper().readTree(parser);
            
//			Due to the JSON structure, "songs" is a JSON node that needs to be 
//          parsed. 
//          My solution here is to get the "songs" as a JSON node 
//          and then replicate the step where we get the actual content.
//          the place holder is for this purpose.             
            Iterator<JsonNode> placeholder = rootNode.iterator(); 
            JsonNode songs = placeholder.next();
            Iterator<JsonNode> iter = songs.iterator();
            
            ObjectNode currentNode;
            
            while (iter.hasNext()) {
                currentNode = (ObjectNode) iter.next();
                
                int year = currentNode.path("year").asInt();
                String title = currentNode.path("title").asText(); 
                
                try {
                    table.putItem(new Item().withPrimaryKey("year", year, "title", title)
                    		.withJSON("artist", currentNode.path("artist").toString())
                    		.withJSON("web_url", currentNode.path("web_url").toString())
                    		.withJSON("image_url", currentNode.path("img_url").toString())
                    		);
                    System.out.println("PutItem succeeded: " + year + " " + title);

                }
                catch (Exception e) {
                    System.err.println("Unable to add movie: " + year + " " + title);
                    System.err.println(e.getMessage());
                    break;
                }
                
            }
            
            parser.close();
    }
    	

}
