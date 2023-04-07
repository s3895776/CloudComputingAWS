package DatabaseManagement;

import java.util.Arrays;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import constants.constants;

/*
 * CreateMusicTable 
 * create a dynamoDB table named "music" 
 * partition key = title
 * sort key = year 
 * Will not create other attributes. 
 * */
public class CreateMusicTable {
	public static void main(String[] args) throws Exception {
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	    		.withRegion(Regions.US_EAST_1)
	    		.withCredentials(new ProfileCredentialsProvider("default"))
	        .build();

	    DynamoDB dynamoDB = new DynamoDB(client);
	    
	    
	    String table_name = constants.MUSIC_TABLE;
	    
	    
	    try {
	    	String partition_key = constants.PARTITION_KEY_MUSIC;
	    	String sort_key = constants.SORT_KEY_MUSIC;
	    	
	        System.out.println("Attempting to create table; please wait...");
	        Table table = dynamoDB.createTable(table_name,
	            Arrays.asList(new KeySchemaElement(partition_key, KeyType.HASH), 
	            		new KeySchemaElement(sort_key, KeyType.RANGE)
	            		),

	            Arrays.asList(new AttributeDefinition(partition_key, ScalarAttributeType.S), 
	            		new AttributeDefinition(sort_key, ScalarAttributeType.S) ),
	            new ProvisionedThroughput(10L, 10L));
	        table.waitForActive();
	        System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

	    }
	    
	    catch (Exception e) {
	        System.err.println("Unable to create login table: ");
	        System.err.println(e.getMessage());
	    }
	    
	}	   
    
}
