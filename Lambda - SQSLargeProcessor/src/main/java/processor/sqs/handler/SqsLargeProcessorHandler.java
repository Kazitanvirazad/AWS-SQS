package processor.sqs.handler;

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class SqsLargeProcessorHandler {
    private static final String BUCKET_NAME = System.getenv("BUCKET_NAME");
    private static final String RESULT_BUCKET_NAME = System.getenv("RESULT_BUCKET_NAME");
    private static final String REGION = System.getenv("REGION");
    private static final String SQS_URL = System.getenv("SQS_URL");
    private static final String ACCESS_KEY = System.getenv("ACCESS_KEY");
    private static final String SECRET_ACCESS_KEY = System.getenv("SECRET_ACCESS_KEY");


    public void handleRequest() {
        Region region = Region.of(REGION);

        AwsBasicCredentials credentials = AwsBasicCredentials.create(ACCESS_KEY, SECRET_ACCESS_KEY);
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

        S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

        ExtendedClientConfiguration sqsExtendedClientConfiguration = new ExtendedClientConfiguration();
        sqsExtendedClientConfiguration.withPayloadSupportEnabled(s3Client, BUCKET_NAME);

        AmazonSQSExtendedClient sqsExtendedClient = new AmazonSQSExtendedClient(sqsClient, sqsExtendedClientConfiguration);

        ReceiveMessageRequest messageRequest = ReceiveMessageRequest
                .builder()
                .queueUrl(SQS_URL)
                .build();

        ReceiveMessageResponse messageResponse = sqsExtendedClient.receiveMessage(messageRequest);

        if (messageResponse.hasMessages()) {
            s3Client.putObject(PutObjectRequest.builder()
                            .bucket(RESULT_BUCKET_NAME)
                            .key(messageResponse.messages().get(0).messageId())
                            .contentType("text/plain").build(),
                    RequestBody.fromString(messageResponse.messages().get(0).body()));
        }
    }
}
