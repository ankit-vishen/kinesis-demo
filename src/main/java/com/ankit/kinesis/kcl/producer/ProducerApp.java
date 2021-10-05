package com.ankit.kinesis.kcl.producer;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ProducerApp {

    private static String accessKey = "access_key";
    private static String secretKey = "secret_key";
    private static String CARD_CHECK_STREAM_NAME = "rummy-card-check-dev";
    private static String PARTITION_KEY = "shard-1";

    public static void main(String[] args) throws InterruptedException {
        /*BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(Regions.AP_SOUTH_1)
                .build();*/

        AmazonKinesis kinesis = AmazonKinesisClientBuilder.defaultClient();

        /*AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setRegion(Regions.AP_SOUTH_1.getName());
        clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCredentials));
        //clientBuilder.setClientConfiguration(config);

        AmazonKinesis kinesisClient = clientBuilder.build();*/

        while(true){
            PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
            putRecordsRequest.setStreamName(CARD_CHECK_STREAM_NAME);
            List <PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
            for (int i = 0; i < 1; i++) {
                PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
                putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(getMoveEvent()).getBytes()));
                putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
                putRecordsRequestEntryList.add(putRecordsRequestEntry);
            }

            putRecordsRequest.setRecords(putRecordsRequestEntryList);
            PutRecordsResult putRecordsResult  = kinesis.putRecords(putRecordsRequest);
            System.out.println("Put Result" + putRecordsResult);

            Thread.sleep(1000);
        }
    }

    public static String getMoveEvent(){
        String moveJson = "{\"pc\":94,\"pd\":{\"groupCount\":5,\"cardDetails\":{\"life1\":{\"groupType\":\"PURE_SEQUENCE\",\"cards\":[92,93,42]},\"group3\":{\"groupType\":\"INVALID_GROUP\",\"cards\":[18,21]},\"group4\":{\"groupType\":\"INVALID_GROUP\",\"cards\":[28,80,35,88]},\"group5\":{\"groupType\":\"INVALID_GROUP\",\"cards\":[41]},\"group6\":{\"groupType\":\"INVALID_GROUP\",\"cards\":[64,65,94]}}},\"dc\":70,\"cd\":{\"groupCount\":5,\"cardDetails\":{\"life1\":{\"groupType\":\"PURE_SEQUENCE\",\"cards\":[92,93,42]},\"group3\":{\"groupType\":\"INVALID_GROUP\",\"cards\":[18,21]},\"group4\":{\"groupType\":\"INVALID_GROUP\",\"cards\":[28,80,35,88]},\"group5\":{\"groupType\":\"INVALID_GROUP\",\"cards\":[41]},\"group6\":{\"groupType\":\"INVALID_GROUP\",\"cards\":[64,65,94]}}},\"j\":95,\"gameId\": \"kaiisidi\",\"userId\": 123}";
        //Gson gson = new Gson();
        //return gson.fromJson(moveJson, MoveEvent.class);
        return  moveJson;
    }
}
