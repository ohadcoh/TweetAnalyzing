package aws;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.ShutdownBehavior;
import com.amazonaws.services.ec2.model.Tag;



public class EC2 {
	
	private static final String keyPair = "ass1KeyPair";
	private static final String securityGroup = "ass1SecurityGroup";
	private AmazonEC2 ec2;
	private AWSCredentials credentials;
	
	public EC2(AWSCredentials credentials){
		this.credentials = credentials;
		this.ec2 = new AmazonEC2Client(this.credentials);

	}
	
	public boolean checkIfManagerExist() {
		DescribeInstancesResult result = ec2.describeInstances();
		List<Reservation> reservations = result.getReservations();
		
		for (Reservation reservation : reservations) {
			List<Instance> instances = reservation.getInstances();
			
			for (Instance instance : instances) {
				List<Tag> tags = instance.getTags();
				
				for (Tag tag : tags){
					if (tag.getKey().equals("Name") && tag.getValue().equals("Manager"))
						return true;
				}
		    }
		}
		return false;
	}
	
	public void startManagerInstance(){
		RunInstancesRequest request = new RunInstancesRequest("ami-08111162", 1, 1);
		request.withKeyName(keyPair);
		request.withSecurityGroups(securityGroup);
		request.withInstanceType(InstanceType.T2Micro);
		//This options terminates the instance on shutdown
		request.withInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate);
		request.withUserData(getUserDataScript("manager"));
		List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
		for (Instance instance : instances) {
			CreateTagsRequest createTagsRequest=new CreateTagsRequest().withResources(instance.getInstanceId()).withTags(new Tag("Name","Manager"));
			ec2.createTags(createTagsRequest);
		}
	}
	
	public void startWorkerInstances(int numOfInstances){
		RunInstancesRequest request = new RunInstancesRequest("ami-08111162", numOfInstances, numOfInstances);
		request.withKeyName(keyPair);
		request.withSecurityGroups(securityGroup);
		request.withInstanceType(InstanceType.T2Micro);
		request.withUserData(getUserDataScript("worker"));
		ec2.runInstances(request).getReservation().getInstances();
	}
	
	private String getUserDataScript(String instanceType){
		ArrayList<String> lines = new ArrayList<String>();
        lines.add("#! /bin/bash");
        lines.add("BIN_DIR=/tmp");
        lines.add("mkdir -p $BIN_DIR/dependencies");
        lines.add("cd $BIN_DIR/dependencies");
        lines.add("wget http://sdk-for-java.amazonwebservices.com/latest/aws-java-sdk.zip");
        lines.add("unzip aws-java-sdk.zip");
        lines.add("mv aws-java-sdk-*/ aws-java-sdk");
        lines.add("cd $BIN_DIR");
        lines.add("mkdir -p $BIN_DIR/bin/jar");
        lines.add("AWS_ACCESS_KEY_ID=" + credentials.getAWSAccessKeyId());
        lines.add("AWS_SECRET_ACCESS_KEY=" + credentials.getAWSSecretKey());
        lines.add("AWS_DEFAULT_REGION=us-east-1");
        lines.add("export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION");
        lines.add("aws s3 cp s3://" + "dsps1testbucket" + "/" + instanceType + ".jar" + instanceType + ".jar");
        lines.add("echo accessKey=$AWS_ACCESS_KEY_ID > asafsarid.properties");
        lines.add("echo secretKey=$AWS_SECRET_ACCESS_KEY >> asafsarid.properties");
        if (instanceType == "manager")
        	lines.add("java -jar testS3.jar");
        else
        	lines.add("java -jar miau.jar");
        String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
	}
	
	static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }
}