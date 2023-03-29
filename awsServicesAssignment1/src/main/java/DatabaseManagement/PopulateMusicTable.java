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
/*
 * Populate music table 
 * populate "music" 
 * partition key = title
 * sort key = year 
 * Will not create other attributes. 
 * */
public class PopulateMusicTable {

    public static void main(String[] args) throws Exception {
    	
    	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            	.withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider("default"))
                .build();

            DynamoDB dynamoDB = new DynamoDB(client);

            Table table = dynamoDB.getTable(constants.MUSIC_TABLE);
           
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
                
                int partition_key = currentNode.path(constants.PARTITION_KEY_MUSIC).asInt();
                String sort_key = currentNode.path(constants.SORT_KEY_MUSIC).asText(); 
                
                try {
                    table.putItem(new Item().withPrimaryKey(constants.PARTITION_KEY_MUSIC, partition_key, constants.SORT_KEY_MUSIC, sort_key)
                    		.withJSON(constants.ARTIST, currentNode.path(constants.ARTIST).toString())
                    		.withJSON(constants.WEB_URL, currentNode.path(constants.WEB_URL).toString())
                    		.withJSON(constants.IMG_URL, currentNode.path(constants.IMG_URL).toString())
                    		);
                    System.out.println("PutItem succeeded: " + partition_key + " " + sort_key);

                }
                catch (Exception e) {
                    System.err.println("Unable to add movie: " + partition_key + " " + sort_key);
                    System.err.println(e.getMessage());
                    break;
                }
                
            }
            
            parser.close();
    }
    	

}
