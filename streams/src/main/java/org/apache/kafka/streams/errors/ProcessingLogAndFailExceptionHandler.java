package org.apache.kafka.streams.errors;

import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProcessingLogAndFailExceptionHandler implements ProcessingExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ProcessingLogAndFailExceptionHandler.class);

    @Override
    public ProcessingHandlerResponse handle(ProcessingContext context, Record<?, ?> record, Exception exception) {
        log.error("Exception caught during Processing, processorNodeId: {}, key: {}, value: {}",
                context.processorNodeId(), record.key(), record.value(), exception);

        return ProcessingHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
