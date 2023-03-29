package DatabaseManagement;

import java.awt.Image;
import java.awt.image.BufferedImage;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
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
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;


public class UploadImageBucket {

    public static void main(String[] args) throws Exception {
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
        
//        use img_url 
        ScanSpec scanSpec = new ScanSpec().withProjectionExpression("title, #yr, img_url")
        		.withNameMap(new NameMap().with("#yr", "year"));
        
        ArrayList<Item> image_urls = new ArrayList<Item>();

        
        try {
        	ItemCollection<ScanOutcome> items = table.scan(scanSpec);
            Iterator<Item> iter = items.iterator();
           
            while (iter.hasNext()) {
            	
                Item item = iter.next();
                image_urls.add(item);
                
            }
        }
        
        catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }
        
        final int NUM_IMAGE_URLS = image_urls.size();
        
        
        URL url;
        BufferedImage image;
        
//        create and write to image files. 
        try {
        	for (int i = 0; i < NUM_IMAGE_URLS; ++i ) {
        		url = new URL( image_urls.get(i).getString(constants.IMG_URL) );
            	image = ImageIO.read(url);

                File outputfile = new File("img_folder\\image" + i +".png");
                ImageIO.write(image, "png", outputfile);
                
        	}
        	
        } catch (IOException e) {
            // handle IOException
        }

        ArrayList<String> imageFiles = new ArrayList<String>();
        
        for (int i = 0; i < image_urls.size(); ++i) {
        	imageFiles.add("img_folder\\image" + i + ".png"); 
        }
        
        String stringObjKeyName = constants.IMG_URL; //name in s3
        String fileObjKeyName = "";//This part can be empty   
        

//      TODO: iterate img_url into the s3 bucket. 
        String bucketName = constants.BUCKET;
        try {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withRegion(clientRegion)
            .build();
            
        	for (int i = 0; i < image_urls.size(); ++i) {
            	

        		fileObjKeyName = constants.IMG_URL + i;
        		
                // Upload an image as a new object with ContentType and title specified. 
        		PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(imageFiles.get(i)) );                             
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("image/png");	
                request.setMetadata(metadata);
                s3Client.putObject(request);
            }
        	
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
        
    }
}
