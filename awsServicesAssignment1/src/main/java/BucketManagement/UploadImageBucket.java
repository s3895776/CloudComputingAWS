package BucketManagement;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;


public class UploadImageBucket {

    public static void main(String[] args) throws IOException {
        Regions clientRegion = Regions.US_EAST_1;
        
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(clientRegion)
                .withCredentials(new ProfileCredentialsProvider("default"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(dynamoClient);

        Table table = dynamoDB.getTable("music");
        
        try {
        	table.waitForActive();
        }
        
        catch (Exception e) {
        	System.err.println("Unable to create music table: ");
  	        System.err.println(e.getMessage());
        }
        
        
        ScanSpec scanSpec = new ScanSpec().withProjectionExpression("title, #yr, image_url");
        
        ArrayList<String> image_urls = new ArrayList<String>();

        
        try {
        	ItemCollection<ScanOutcome> items = table.scan(scanSpec);
            Iterator<Item> iter = items.iterator();
            
//            System.out.println( iter.hasNext() );
            
//          
            while (iter.hasNext()) {
            	
                Item item = iter.next();
                image_urls.add(item.toString());
                
            }
        }
        
        catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }
        
        for (int i = 0; i < image_urls.size(); ++i ){
        	System.out.print(image_urls.get(i) + ", ");
        }
        System.out.println();
        
//      AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//      .withRegion(clientRegion)
//      .build();
//        
////        TODO: iterate image_urls into the s3 bucket. 
//        String stringObjKeyName = "image_url"; //name in s3
//        String fileObjKeyName = "";//This part can be empty
//        String fileName = "*** Path to file to upload ***";//e.g., sample.txt
//        
//        for (int i = 0; i < 0; ++i) {
//        	
//        	String stringObjKeyNameIterate =  "image_url" + i;
//        	String fileNameIterate = "example_url"; 
//
//        }
//        
//        String bucketName = "s3895776imagebucket";
//        try {
//            //This code expects that you have AWS credentials set up per:
//            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
//            
//
//            // Upload a text string as a new object.
//            s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");
//
//            // Upload a file as a new object with ContentType and title specified.
//            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
//            ObjectMetadata metadata = new ObjectMetadata();
//            metadata.setContentType("plain/text");	
//            metadata.addUserMetadata("title", "someTitle");
//            request.setMetadata(metadata);
//            s3Client.putObject(request);
//        } catch (AmazonServiceException e) {
//            // The call was transmitted successfully, but Amazon S3 couldn't process 
//            // it, so it returned an error response.
//            e.printStackTrace();
//        } catch (SdkClientException e) {
//            // Amazon S3 couldn't be contacted for a response, or the client
//            // couldn't parse the response from Amazon S3.
//            e.printStackTrace();
//        }
        
    }
}
