package com.ankit.kinesis.kcl.consumer;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.ankit.kinesis.kcl.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class ConsumerApp {

    private static final InitialPositionInStream INITIAL_POSITION_IN_STREAM = InitialPositionInStream.LATEST;

    private static String CARD_CHECK_STREAM_NAME = "rummy-card-check-dev";

    private static String APPLICATION_NAME = "card-check-app";

    public static void main(String[] args) throws UnknownHostException {

        // Set AWS credentials
        AWSCredentialsProvider credentialsProvider = Util.initCredentialsProvider();

        // Set KCL configuration
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(APPLICATION_NAME, CARD_CHECK_STREAM_NAME,
                credentialsProvider, workerId);
        kclConfiguration.withRegionName(Regions.AP_SOUTH_1.getName());
        kclConfiguration.withInitialPositionInStream(INITIAL_POSITION_IN_STREAM);

        // Start workers
        IRecordProcessorFactory recordProcessorFactory = new CardConsumerFactory();
        IMetricsFactory metricsFactory = new NullMetricsFactory();
        Worker worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration)
                .metricsFactory(metricsFactory).build();
        try {
            System.out.printf("Running %s to process stream %s as worker %s...\n", APPLICATION_NAME, CARD_CHECK_STREAM_NAME,
                    workerId);
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            System.exit(1);
        }
    }
}
