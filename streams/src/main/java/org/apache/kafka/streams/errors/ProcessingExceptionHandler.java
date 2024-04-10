package org.apache.kafka.streams.errors;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.processor.api.ProcessingContext;
import org.apache.kafka.streams.processor.api.Record;

public interface ProcessingExceptionHandler extends Configurable {
    ProcessingHandlerResponse handle(ProcessingContext context, Record<Object, Object> record, Exception exception);

    enum ProcessingHandlerResponse {
        CONTINUE(0, "CONTINUE"),
        FAIL(1, "FAIL");

        public final String name;

        public final int id;

        ProcessingHandlerResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }
}