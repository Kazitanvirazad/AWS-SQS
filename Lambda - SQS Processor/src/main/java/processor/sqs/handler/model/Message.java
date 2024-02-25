package processor.sqs.handler.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

import java.io.Serial;
import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@DynamoDbBean
public class Message implements Serializable {
    @Serial
    private static final long serialVersionUID = -6933146513372356384L;
    private String id;
    private String messageId;
    private String message;

    @DynamoDbPartitionKey
    public String getId() {
        return id;
    }
}
