package processor.sqs.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.sqs.model.Message;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class SnsSqsProcessorHandler implements RequestHandler<SQSEvent, Boolean> {
    private static Logger LOGGER = LogManager.getLogger(SnsSqsProcessorHandler.class);
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
        Region region = Region.of(REGION);
        String snsMessage = sqsMessage.getBody();
        TypeReference<HashMap<String, Object>> typeReference = new TypeReference<>() {
        };
        ObjectMapper mapper = new ObjectMapper();

        try (DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .region(region)
                .build()) {
            DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                    .dynamoDbClient(dynamoDbClient).build();
            DynamoDbTable<Message> table = enhancedClient.table(TABLE_NAME,
                    TableSchema.fromBean(Message.class));
            HashMap<String, Object> snsTopicMap = mapper.readValue(snsMessage, typeReference);

            Message message = new Message();
            message.setId(UUID.randomUUID().toString());
            message.setMessageId(sqsMessage.getMessageId());
            message.setMessage((String) snsTopicMap.get("Message"));
            message.setSubject((String) snsTopicMap.get("Subject"));
            table.putItem(message);
        } catch (UnsupportedOperationException | JsonProcessingException exception) {
            LOGGER.error(exception.getMessage());
        }
    }
}
