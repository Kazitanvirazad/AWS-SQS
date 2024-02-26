package publisher.sns.handler;

import publisher.sns.util.Payload;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SetSubscriptionAttributesRequest;
import software.amazon.sns.AmazonSNSExtendedClient;
import software.amazon.sns.SNSExtendedClientConfiguration;

public class SnsLargePublisherHandler {
    private static final String BUCKET_NAME = System.getenv("BUCKET_NAME");
    private static final String REGION = System.getenv("REGION");
    private static final String TOPIC_ARN = System.getenv("TOPIC_ARN");
    private static final String SUBSCRIBTION_ARN = System.getenv("SUBSCRIBTION_ARN");

    public String handleRequest() {
        Region region = Region.of(REGION);
        String largePayload = Payload.getPayload();

        SnsClient snsClient = SnsClient.builder()
                .region(region)
                .build();
        S3Client s3Client = S3Client.builder()
                .region(region)
                .build();

        SetSubscriptionAttributesRequest setSubscriptionAttributesRequest =
                SetSubscriptionAttributesRequest.builder()
                        .subscriptionArn(SUBSCRIBTION_ARN)
                        .attributeName("RawMessageDelivery")
                        .attributeValue("TRUE")
                        .build();
        snsClient.setSubscriptionAttributes(setSubscriptionAttributesRequest);

        SNSExtendedClientConfiguration snsExtendedClientConfiguration
                = new SNSExtendedClientConfiguration();
        snsExtendedClientConfiguration.withPayloadSupportEnabled(s3Client, BUCKET_NAME);
        snsExtendedClientConfiguration.setPayloadSizeThreshold(256);

        AmazonSNSExtendedClient snsExtendedClient = new AmazonSNSExtendedClient(snsClient, snsExtendedClientConfiguration);

        PublishRequest publishRequest = PublishRequest.builder()
                .targetArn(TOPIC_ARN)
                .subject("Large Payload")
                .message(largePayload)
                .build();
        snsExtendedClient.publish(publishRequest);

        return "Message published";
    }
}
