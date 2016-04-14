package remote;

import java.io.File;

import aws.S3;
import aws.SQS;

public class Manager {
	private SQS localToManager;
	private SQS ManagerToWorker;
	private SQS WorkerToManager;
	private S3 s3;
	private File inputFile;
	private File outputFile;
	
	public Manager() {
		super();
	}
	
	
}
