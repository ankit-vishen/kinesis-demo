package com.ankit.kinesis.kcl.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class CardConsumerFactory implements IRecordProcessorFactory {
    @Override
    public IRecordProcessor createProcessor() {
        return new CardConsumer();
    }
}
