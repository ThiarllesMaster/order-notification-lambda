package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

public class OrderNotificationHandler  implements RequestHandler<DynamodbEvent, String> {

    private final SnsClient snsClient = SnsClient.builder()
            .region(Region.of(System.getenv("AWS_REGION")))
            .build();

    private final String topicArn =
            System.getenv("SNS_TOPIC_ARN");

    @Override
    public String handleRequest(
            DynamodbEvent event,
            Context context) {

        var logger = context.getLogger();

        if (event == null || event.getRecords() == null) {
            logger.log("Evento sem registos do DynamoDB Streams.\n");
            return "No records";
        }

        for (DynamodbEvent.DynamodbStreamRecord record
                : event.getRecords()) {

            if (record.getEventName() == null
                    || !"INSERT".equals(record.getEventName())) {
                continue;
            }

            var dynamodb = record.getDynamodb();

            if (dynamodb == null || dynamodb.getNewImage() == null) {
                logger.log("Registo sem NewImage.\n");
                continue;
            }

            var image = dynamodb.getNewImage();

            String orderId = getString(image, "id");
            String customerId = getString(image, "customerId");
            String product = getString(image, "product");
            String quantity = getNumber(image, "quantity");
            String total = getNumber(image, "total");
            String status = getString(image, "status");

            String message = """
                    {
                      "eventType": "ORDER_CREATED",
                      "order": {
                        "id": "%s",
                        "customerId": "%s",
                        "product": "%s",
                        "quantity": %s,
                        "total": %s,
                        "status": "%s"
                      }
                    }
                    """.formatted(
                    escapeJson(orderId),
                    escapeJson(customerId),
                    escapeJson(product),
                    validNumber(quantity),
                    validNumber(total),
                    escapeJson(status)
            );

            PublishRequest request = PublishRequest.builder()
                    .topicArn(topicArn)
                    .subject("Novo pedido criado")
                    .message(message)
                    .build();

            snsClient.publish(request);

            logger.log("Notificação SNS publicada para o pedido: "
                    + orderId + "\n");
        }

        return "Processing completed";
    }

    private static String getString(
            java.util.Map<String,
                    com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue> image,
            String key) {

        var attribute = image.get(key);
        return attribute == null || attribute.getS() == null
                ? ""
                : attribute.getS();
    }

    private static String getNumber(
            java.util.Map<String,
                    com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue> image,
            String key) {

        var attribute = image.get(key);
        return attribute == null || attribute.getN() == null
                ? "0"
                : attribute.getN();
    }

    private static String validNumber(String value) {
        try {
            return new java.math.BigDecimal(value).toPlainString();
        } catch (NumberFormatException exception) {
            return "0";
        }
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}