package processor.sqs.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import lombok.extern.slf4j.Slf4j;
import processor.sqs.handler.model.Message;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.List;
import java.util.UUID;

@Slf4j
public class SQSProcessorHandler implements RequestHandler<SQSEvent, Boolean> {
    private static final String REGION = System.getenv("REGION");
    private static final String TABLE_NAME = System.getenv("TABLE_NAME");


    @Override
    public Boolean handleRequest(SQSEvent sqsEvent, Context context) {
        List<SQSEvent.SQSMessage> records = sqsEvent.getRecords();
        for (SQSEvent.SQSMessage sqsMessage : records) {
            processSQSRecord(sqsMessage);
        }
        return true;
    }

    private void processSQSRecord(SQSEvent.SQSMessage sqsMessage) {
        Message message = new Message(UUID.randomUUID().toString(), sqsMessage.getMessageId(),
                sqsMessage.getBody());

        Region region = Region.of(REGION);

        try (DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .region(region)
                .build()) {
            DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                    .dynamoDbClient(dynamoDbClient).build();
            DynamoDbTable<Message> table = enhancedClient.table(TABLE_NAME,
                    TableSchema.fromBean(Message.class));
            table.putItem(message);
        } catch (UnsupportedOperationException exception) {
            log.error(exception.getMessage());
        }
    }
}
