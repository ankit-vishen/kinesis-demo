package com.ankit.kinesis.kcl.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CardConsumer implements IRecordProcessor {

    private static final boolean IF_TIME_FIELD_ENABLED = Boolean.parseBoolean(System.getProperty("if.time.field.enabled", "false"));
    private static final String TIME_FIELD_NAME = System.getProperty("time.field.name", "time");
    private static final String TIME_FIELD_FORMAT = System.getProperty("time.field.format", "yyyy-MM-dd'T'HH:mm:ss.SSSxxxxx");


    private String shardId;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    private final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(TIME_FIELD_FORMAT);
    private final ObjectMapper mapper = new ObjectMapper();

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    @Override
    public void initialize(String shardId) {
        System.out.println("init: shardId "+shardId);
        this.shardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {
        System.out.println("Processing " + records.size() + " records from " + shardId);

        // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(iRecordProcessorCheckpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {
        System.out.println("Shutting down record processor for shard: " + shardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownReason == ShutdownReason.TERMINATE) {
            checkpoint(iRecordProcessorCheckpointer);
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        System.out.println("Checkpointing shard " + shardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                System.out.println("Caught shutdown exception, skipping checkpoint. "+se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    System.out.println("Checkpoint failed after " + (i + 1) + "attempts. "+e);
                    break;
                } else {
                    System.out.println("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES+" "+e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                System.out.println("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library. "+e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                System.out.println("Interrupted sleep "+e);
            }
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records
     *            Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    processSingleRecord(record);
                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    System.out.println("Caught throwable while processing record " + record +" t="+t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    //LOG.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                System.out.println("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
     * Process a single record.
     *
     * @param record
     *            The record to be processed.
     */
    private void processSingleRecord(Record record) {
        String data = null;
        try {
            // For this app, we interpret the payload as UTF-8 chars.
            data = decoder.decode(record.getData()).toString();

            long approximateArrivalTimestamp = record.getApproximateArrivalTimestamp().getTime();
            long currentTime = System.currentTimeMillis();
            long ageOfRecordInMillisFromArrival = currentTime - approximateArrivalTimestamp;

            System.out.println("data}}}}} ---\nShard: " + shardId + ", PartitionKey: " + record.getPartitionKey() + ", SequenceNumber: "
                    + record.getSequenceNumber() + " milliseconds ago. Arrived "
                    + ageOfRecordInMillisFromArrival + " milliseconds ago.\n data->" + data);

            // Assume this record including time field and log its age.
            /*JsonNode jsonData = mapper.readTree(data);
            long approximateArrivalTimestamp = record.getApproximateArrivalTimestamp().getTime();
            long currentTime = System.currentTimeMillis();
            long ageOfRecordInMillisFromArrival = currentTime - approximateArrivalTimestamp;
            if (IF_TIME_FIELD_ENABLED) {
                long recordCreateTime = ZonedDateTime.parse(jsonData.get(TIME_FIELD_NAME).asText(), dtf).toInstant().toEpochMilli();
                long ageOfRecordInMillis = currentTime - recordCreateTime;
                System.out.println("data}}}}} ---\nShard: " + shardId + ", PartitionKey: " + record.getPartitionKey() + ", SequenceNumber: "
                        + record.getSequenceNumber() + "\nCreated " + ageOfRecordInMillis + " milliseconds ago. Arrived "
                        + ageOfRecordInMillisFromArrival + " milliseconds ago.\n data->" + data);
            } else {
                System.out.println("data}}}}} ---\nShard: " + shardId + ", PartitionKey: " + record.getPartitionKey() + ", SequenceNumber: "
                        + record.getSequenceNumber() + "\nArrived " + ageOfRecordInMillisFromArrival + " milliseconds ago.\n data->" + data);
            }*/
        } catch (CharacterCodingException e) {
            System.out.println("Malformed data: " + data +" e "+e);
        } catch (IOException e) {
            System.out.println("Record does not match sample record format. Ignoring record with data; " + data);
        }
    }
}
