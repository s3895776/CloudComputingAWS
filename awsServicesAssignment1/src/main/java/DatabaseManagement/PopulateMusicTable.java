package DatabaseManagement;

import java.io.File;
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
            Iterator<JsonNode> iter = rootNode.iterator();  
//            JSON file is structured differently so this does not work correctly.
           
            
            ObjectNode currentNode;
            
            while (iter.hasNext()) {
                currentNode = (ObjectNode) iter.next();
                
                int year = currentNode.path("year").asInt();
                String title = currentNode.path("title").asText(); 
                
                try {
                    table.putItem(new Item().withPrimaryKey("year", year, "title", title)
                    		.withJSON("artist", currentNode.path("artist").toString())
                    		.withJSON("web_url", currentNode.path("web_url").toString())
                    		.withJSON("image_url", currentNode.path("image_url").toString())
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
