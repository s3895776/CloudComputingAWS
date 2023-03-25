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

public class CreateMusicTable {
	public static void main(String[] args) throws Exception {
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	    		.withRegion(Regions.US_EAST_1)
	    		.withCredentials(new ProfileCredentialsProvider("default"))
	        .build();

	    DynamoDB dynamoDB = new DynamoDB(client);
	    
	    String table_name = "music";
	    
	    try {
	        System.out.println("Attempting to create table; please wait...");
	        Table table = dynamoDB.createTable(table_name,
	            Arrays.asList(new KeySchemaElement("title", KeyType.HASH), new KeySchemaElement("year", KeyType.RANGE)),
	            Arrays.asList(new AttributeDefinition("title", ScalarAttributeType.S), new AttributeDefinition("year", ScalarAttributeType.N)),
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
