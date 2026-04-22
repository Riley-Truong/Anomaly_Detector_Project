import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
 
public class AnomalyDetectorReader {
 
    public static void main(String[] args) {
        String today = LocalDate.now().toString();
 
        DynamoDbClient db = DynamoDbClient.builder()
            .region(Region.US_EAST_1)
            .build();
 
        ScanRequest request = ScanRequest.builder()
            .tableName("Anomaly_Detector_Results")
            .filterExpression("#d = :today")
            .expressionAttributeNames(Map.of("#d", "date"))
            .expressionAttributeValues(Map.of(
                ":today", AttributeValue.fromS(today)))
            .build();
 
        List<Map<String, AttributeValue>> items = db.scan(request).items();
 
        // Sort by absolute difference descending (most anomalous first)
        items.sort(Comparator.comparingDouble(item ->
            -Math.abs(Double.parseDouble(item.get("difference").s()))));
 
        System.out.println("\nAnomaly Detector Results for " + today);
        System.out.println("=".repeat(65));
        System.out.printf("%-20s %8s %8s %10s %8s%n",
            "City", "Today", "Avg", "Diff", "Flag");
        System.out.println("-".repeat(65));
 
        for (Map<String, AttributeValue> item : items) {
            double diff    = Double.parseDouble(item.get("difference").s());
            boolean flagged = item.get("flagged").bool();
            System.out.printf("%-20s %7.1fF %7.1fF %+9.1f %8s%n",
                item.get("city").s(),
                Double.parseDouble(item.get("current_temp").s()),
                Double.parseDouble(item.get("avg_temp").s()),
                diff,
                flagged ? "FLAGGED" : "-"
            );
        }
        System.out.println("=".repeat(65));
        long flagCount = items.stream()
            .filter(i -> i.get("flagged").bool()).count();
        System.out.println(flagCount + " cities flagged today.");
    }
}