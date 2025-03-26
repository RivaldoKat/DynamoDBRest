package com.aws.rest;

import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class DynamoDBService {

    private DynamoDbClient getClient() {
        Region region = Region.EU_NORTH_1;
        return DynamoDbClient.builder().region(region)
                .credentialsProvider(ProfileCredentialsProvider.create("528757827098_AdministratorAccess"))
                .build();
    }

    // Get all items from the table
    public List<WorkItem> getAllItems() {
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(getClient()).build();

        try{
            DynamoDbTable<Work> table = enhancedClient.table("Work", TableSchema.fromBean(Work.class));
            Iterator<Work> results = table.scan().items().iterator();
            WorkItem workItem;
            ArrayList<WorkItem> itemList = new ArrayList<>();

            while(results.hasNext()) {
                workItem = new WorkItem();
                Work work = results.next();
                workItem.setName(work.getName());
                workItem.setGuide(work.getGuide());
                workItem.setDescription(work.getDescription());
                workItem.setStatus(work.getStatus());
                workItem.setDate(work.getDate());
                workItem.setId(work.getId());
                workItem.setArchived(workItem.getArchived());

                // Push the workItem to the list
                itemList.add(workItem);
            }

            return itemList;
        }
        catch(DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return null;
    }

    // Archives an item based on the key
    public void archiveItemEC(String id) {
        try{
            DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(getClient())
                    .build();

            DynamoDbTable<Work> workTable = enhancedClient.table("Work", TableSchema.fromBean(Work.class));

            // Get the key object
            Key key = Key.builder().partitionValue(id).build();

            // Get the item by using the key
            Work work = workTable.getItem(r->r.key(key));
            work.setArchive(1);
            workTable.updateItem(r->r.item(work));
        }
        catch(DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    // Get Open items from the DynamoBD table
    public List<WorkItem> getOpenItems() {
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(getClient())
                .build();

        try{
            DynamoDbTable<Work> workTable = enhancedClient.table("Work", TableSchema.fromBean(Work.class));
            AttributeValue attributeValue = AttributeValue.builder().n("0").build();

            Map<String, AttributeValue> myMap = new HashMap<>();
            myMap.put(":val1", attributeValue);

            Map<String, String> myExMap = new HashMap<>();
            myExMap.put("#archive", "archive");

            // Set the expression so only active items are queried from the work table
            Expression expression = Expression.builder().expressionValues(myMap).expressionNames(myExMap)
                    .expression("#archive = :val1")
                    .build();

            ScanEnhancedRequest enhancedRequest = ScanEnhancedRequest.builder().filterExpression(expression)
                    .limit(15).build();

            // scan items
            Iterator<Work> results = workTable.scan(enhancedRequest).items().iterator();
            WorkItem workItem;
            ArrayList<WorkItem> itemList = new ArrayList<>();

            while(results.hasNext()) {
                workItem = new WorkItem();
                Work work = results.next();
                workItem.setName(work.getName());
                workItem.setGuide(work.getGuide());
                workItem.setDescription(work.getDescription());
                workItem.setStatus(work.getStatus());
                workItem.setDate(work.getDate());
                workItem.setId(work.getId());
                workItem.setArchived(workItem.getArchived());

                // Push the workItem to the list
                itemList.add(workItem);
            }

            return itemList;
        }
        catch (DynamoDbException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return null;
    }

    // Get closed items from the dynamodb table
    public List<WorkItem> getClosedItems() {

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(getClient())
                .build();

        try{
            // Create a DynamoDBTable object
            DynamoDbTable<Work> table= enhancedClient.table("work", TableSchema.fromBean(Work.class));
            AttributeValue attributeValue = AttributeValue.builder()
                    .n("1")
                    .build();

            Map<String, AttributeValue> myMap = new HashMap<>();
            myMap.put(":val1", attributeValue);
            Map<String, String> myExMAp = new HashMap<>();
            myExMAp.put("#archive", "archive");

            // Set the expression so only closed items are queried from the work table
            Expression expression = Expression.builder()
                    .expressionValues(myMap)
                    .expressionNames(myExMAp)
                    .expression("#archive1 = :val1")
                    .build();

            ScanEnhancedRequest enhancedRequest = ScanEnhancedRequest.builder()
                    .filterExpression(expression)
                    .limit(15)
                    .build();

            // Get items
            Iterator<Work> results = table.scan(enhancedRequest).items().iterator();
            WorkItem workItem;
            ArrayList<WorkItem> itemList = new ArrayList<>();

            while(results.hasNext()) {
                workItem = new WorkItem();
                Work work = results.next();
                workItem.setName(work.getName());
                workItem.setGuide(work.getGuide());
                workItem.setDescription(work.getDescription());
                workItem.setStatus(work.getStatus());
                workItem.setDate(work.getDate());
                workItem.setId(work.getId());
                workItem.setArchived(workItem.getArchived());

                // Push the workItem to the list
                itemList.add(workItem);
            }

            return itemList;
        }
        catch(DynamoDbException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return null;
    }

    public void setItem(WorkItem item) {
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(getClient())
                .build();

        putRecord(enhancedClient, item);
    }

    private void putRecord(DynamoDbEnhancedClient enhancedClient, WorkItem item) {

        try {
            DynamoDbTable<Work> workTable = enhancedClient.table("Work", TableSchema.fromBean(Work.class));
            String myGuid = java.util.UUID.randomUUID().toString();
            Work record = new Work();
            record.setUsername(item.getName());
            record.setId(myGuid);
            record.setDescription(item.getDescription());
            record.setDate(now());
            record.setStatus(item.getStatus());
            record.setArchive(0);
            record.setGuide(item.getGuide());
            workTable.putItem(record);

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }


    }

    private String now() {
        String dateFormatNow = "yyyy-MM-dd HH:mm:ss";
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormatNow);
        return simpleDateFormat.format(cal.getTime());
    }
}
