package DatabaseManagement;

import java.util.ArrayList;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

public class PopulateLoginTable {

	public static void main(String[] args) throws Exception {

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1)
				.withCredentials(new ProfileCredentialsProvider("default")).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable("login");

		String student_number = "s3895776";
		String end_of_email = "@student.rmit.edu.au";
		String my_name = "Guo An Liew";

		ArrayList<String> emails = new ArrayList<String>();
		ArrayList<String> usernames = new ArrayList<String>();
		ArrayList<String> passwords = new ArrayList<String>();

		for (int i = 0; i < 10; ++i) {

			// may not have automatic type conversion here
			emails.add(student_number + i + end_of_email);
			usernames.add(my_name + Integer.toString(i));
			passwords.add(Integer.toString(i) + Integer.toString( (i + 1) % 10 ) + Integer.toString((i + 2) % 10)
					+ Integer.toString((i + 3) % 10) + Integer.toString((i + 4) % 10) + Integer.toString((i + 5) % 10));
		}


		try {
			
			for (int i = 0; i < 10; ++i) {
				
				Item item = new Item().withPrimaryKey("email", emails.get(i))
						.withString("password", passwords.get(i))
						.withString("username", usernames.get(i) );				
				System.out.println("Adding a new item...");
				PutItemOutcome outcome = table.putItem(item);
				System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

			}
			
		} catch (Exception e) {
			System.err.println("Unable to add emails, names and passwords.");
			System.err.println(e.getMessage());
		}

		
	}
}
