/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class ProcessingExceptionHandlerIntegrationTest {
    private final String failingProcessorName = "FAILING-PROCESSOR";
    private final String threadId = Thread.currentThread().getName();
    private MockProcessorSupplier<String, String, Void, Void> processor;

    @BeforeEach
    void setUp() {
        // Reset processor count
        processor = new MockProcessorSupplier<>();
    }

    @Test
    void shouldFailWhenProcessingExceptionOccursInTopologyWithMultipleStatelessNodes() {
        final StreamsBuilder streamsBuilder = buildTopologyWithMultipleStatelessNodes();
        runAndFailStreamsOnProcessingException(streamsBuilder);
    }

    @Test
    void shouldContinueWhenProcessingExceptionOccursInTopologyWithMultipleStatelessNodes() {
        final StreamsBuilder streamsBuilder = buildTopologyWithMultipleStatelessNodes();
        runAndContinueStreamsOnProcessingException(streamsBuilder);
    }

    @Test
    void shouldFailWhenProcessingExceptionOccursInTopologyWithCachingKeyValueStore() {
        final StreamsBuilder streamsBuilder = buildTopologyWithCachingKeyValueStore();
        runAndFailStreamsOnProcessingException(streamsBuilder);
    }

    @Test
    void shouldContinueWhenProcessingExceptionOccursInTopologyWithCachingKeyValueStore() {
        final StreamsBuilder streamsBuilder = buildTopologyWithCachingKeyValueStore();
        runAndContinueStreamsOnProcessingException(streamsBuilder);
    }

    @Test
    void shouldFailWhenProcessingExceptionOccursInTopologyWithCachingWindowStore() {
        final StreamsBuilder streamsBuilder = buildTopologyWithCachingWindowStore();
        runAndFailStreamsOnProcessingException(streamsBuilder);
    }

    @Test
    void shouldContinueWhenProcessingExceptionOccursInTopologyWithCachingWindowStore() {
        final StreamsBuilder streamsBuilder = buildTopologyWithCachingWindowStore();
        runAndContinueStreamsOnProcessingException(streamsBuilder);
    }

    private StreamsBuilder buildTopologyWithMultipleStatelessNodes() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .<String, String>stream("TOPIC_NAME")
            .map(KeyValue::new)
            .mapValues(value -> value)
            .process(runtimeErrorProcessorSupplierMock(), Named.as(failingProcessorName))
            .process(processor);
        return streamsBuilder;
    }

    private StreamsBuilder buildTopologyWithCachingKeyValueStore() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .<String, String>stream("TOPIC_NAME")
            .groupByKey()
            .aggregate(() -> "", (key, value, aggregate) -> value)
            .toStream()
            .process(runtimeErrorProcessorSupplierMock(), Named.as(failingProcessorName))
            .process(processor);
        return streamsBuilder;
    }

    private StreamsBuilder buildTopologyWithCachingWindowStore() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .<String, String>stream("TOPIC_NAME")
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .aggregate(() -> "", (key, value, aggregate) -> value)
            .toStream()
            .selectKey((key, value) -> key.key())
            .process(runtimeErrorProcessorSupplierMock(), Named.as(failingProcessorName))
            .process(processor);
        return streamsBuilder;
    }

    private void runAndFailStreamsOnProcessingException(final StreamsBuilder streamsBuilder) {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2-ERR", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4")
        );

        final List<KeyValueTimestamp<String, String>> expectedProcessedRecords = Collections.singletonList(
            new KeyValueTimestamp<>("ID123-1", "ID123-A1", 0)
        );

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, FailProcessingExceptionHandlerMockTest.class);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        try (final TopologyTestDriver driver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());

            final StreamsException exception = assertThrows(StreamsException.class,
                () -> inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO));

            assertInstanceOf(RuntimeException.class, exception.getCause());
            assertEquals("Exception should be handled by processing exception handler", exception.getCause().getMessage());
            assertEquals(expectedProcessedRecords.size(), processor.theCapturedProcessor().processed().size());
            assertIterableEquals(expectedProcessedRecords, processor.theCapturedProcessor().processed());

            final MetricName dropTotal = droppedRecordsTotalMetric();
            final MetricName dropRate = droppedRecordsRateMetric();

            assertEquals(0.0, driver.metrics().get(dropTotal).metricValue());
            assertEquals(0.0, driver.metrics().get(dropRate).metricValue());
        }
    }

    private void runAndContinueStreamsOnProcessingException(final StreamsBuilder streamsBuilder) {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2-ERR", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4"),
            new KeyValue<>("ID123-5-ERR", "ID123-A5"),
            new KeyValue<>("ID123-6", "ID123-A6")
        );

        final List<KeyValueTimestamp<String, String>> expectedProcessedRecords = Arrays.asList(
            new KeyValueTimestamp<>("ID123-1", "ID123-A1", 0),
            new KeyValueTimestamp<>("ID123-3", "ID123-A3", 0),
            new KeyValueTimestamp<>("ID123-4", "ID123-A4", 0),
            new KeyValueTimestamp<>("ID123-6", "ID123-A6", 0)
        );

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, ContinueProcessingExceptionHandlerMockTest.class);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        try (final TopologyTestDriver driver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO);

            assertEquals(expectedProcessedRecords.size(), processor.theCapturedProcessor().processed().size());
            assertIterableEquals(expectedProcessedRecords, processor.theCapturedProcessor().processed());

            final MetricName dropTotal = droppedRecordsTotalMetric();
            final MetricName dropRate = droppedRecordsRateMetric();

            assertEquals(2.0, driver.metrics().get(dropTotal).metricValue());
            assertTrue((Double) driver.metrics().get(dropRate).metricValue() > 0.0);
        }
    }

    public static class ContinueProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertProcessingExceptionHandlerInputs(context, record, exception, "FAILING-PROCESSOR");
            return ProcessingExceptionHandler.ProcessingHandlerResponse.CONTINUE;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    public static class FailProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertProcessingExceptionHandlerInputs(context, record, exception, "FAILING-PROCESSOR");
            return ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    private static void assertProcessingExceptionHandlerInputs(final ErrorHandlerContext context,
                                                               final Record<?, ?> record,
                                                               final Exception exception,
                                                               final String failingProcessorName) {
        assertTrue(Arrays.asList("ID123-2-ERR", "ID123-5-ERR").contains(new String(context.sourceRawKey())));
        assertTrue(Arrays.asList("ID123-A2", "ID123-A5").contains(new String(context.sourceRawValue())));
        assertTrue(Arrays.asList("ID123-2-ERR", "ID123-5-ERR").contains((String) record.key()));
        assertTrue(Arrays.asList("ID123-A2", "ID123-A5").contains((String) record.value()));
        assertEquals("TOPIC_NAME", context.topic());
        assertEquals(failingProcessorName, context.processorNodeId());
        assertTrue(exception.getMessage().contains("Exception should be handled by processing exception handler"));
    }

    /**
     * Metric name for dropped records total.
     *
     * @return the metric name
     */
    private MetricName droppedRecordsTotalMetric() {
        return new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        );
    }

    /**
     * Metric name for dropped records rate.
     *
     * @return the metric name
     */
    private MetricName droppedRecordsRateMetric() {
        return new MetricName(
            "dropped-records-rate",
            "stream-task-metrics",
            "The average number of dropped records per second",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        );
    }

    /**
     * Processor supplier that throws a runtime exception on process.
     *
     * @return the processor supplier
     */
    private ProcessorSupplier<String, String, String, String> runtimeErrorProcessorSupplierMock() {
        return () -> new ContextualProcessor<String, String, String, String>() {
            @Override
            public void process(final Record<String, String> record) {
                if (record.key().contains("ERR")) {
                    throw new RuntimeException("Exception should be handled by processing exception handler");
                }

                context().forward(new Record<>(record.key(), record.value(), record.timestamp()));
            }
        };
    }
}