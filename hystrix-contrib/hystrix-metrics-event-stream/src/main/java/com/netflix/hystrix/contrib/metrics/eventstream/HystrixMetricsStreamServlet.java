/**
 * Copyright 2012 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.contrib.metrics.eventstream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.*;
import com.netflix.hystrix.util.HystrixRollingNumberEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Streams Hystrix metrics in text/event-stream format.
 * <p>
 * Install by:
 * <p>
 * 1) Including hystrix-metrics-event-stream-*.jar in your classpath.
 * <p>
 * 2) Adding the following to web.xml:
 * <pre>{@code
 * <servlet>
 *  <description></description>
 *  <display-name>HystrixMetricsStreamServlet</display-name>
 *  <servlet-name>HystrixMetricsStreamServlet</servlet-name>
 *  <servlet-class>com.netflix.hystrix.contrib.metrics.eventstream.HystrixMetricsStreamServlet</servlet-class>
 * </servlet>
 * <servlet-mapping>
 *  <servlet-name>HystrixMetricsStreamServlet</servlet-name>
 *  <url-pattern>/hystrix.stream</url-pattern>
 * </servlet-mapping>
 * } </pre>
 */
public class HystrixMetricsStreamServlet extends HttpServlet {

    private static final long serialVersionUID = -7548505095303313237L;

    private static final Logger logger = LoggerFactory.getLogger(HystrixMetricsStreamServlet.class);

    /* used to track number of connections and throttle */
    private static AtomicInteger concurrentConnections = new AtomicInteger(0);
    private static DynamicIntProperty maxConcurrentConnections = DynamicPropertyFactory.getInstance().getIntProperty("hystrix.stream.maxConcurrentConnections", 5);

    private static volatile boolean isDestroyed = false;

    private static final JsonFactory jsonFactory = new JsonFactory();

    static {
        jsonFactory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    }
    
    /**
     * WebSphere won't shutdown a servlet until after a 60 second timeout if there is an instance of the servlet executing 
     * a request.  Add this method to enable a hook to notify Hystrix to shutdown.  You must invoke this method at
     * shutdown, perhaps from some other serverlet's destroy() method.
     */
    public static void shutdown() {
    	isDestroyed = true;
    }
    
    @Override 
    public void init() throws ServletException {
    	isDestroyed = false;
    }
    
    /**
     * Handle incoming GETs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        handleRequest(request, response);
    }
    
    /**
     * Handle servlet being undeployed by gracefully releasing connections so poller threads stop.
     */
    @Override
    public void destroy() {
        /* set marker so the loops can break out */
        isDestroyed = true;
        super.destroy();
    }

    /**
     * - maintain an open connection with the client
     * - on initial connection send latest data of each requested event type
     * - subsequently send all changes for each requested event type
     * 
     * @param request
     * @param response
     * @throws javax.servlet.ServletException
     * @throws java.io.IOException
     */
    private void handleRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        /* ensure we aren't allowing more connections than we want */
        int numberConnections = concurrentConnections.incrementAndGet();
        try {
            if (numberConnections > maxConcurrentConnections.get()) {
                response.sendError(503, "MaxConcurrentConnections reached: " + maxConcurrentConnections.get());
            } else {

                int delay = 500;
                try {
                    String d = request.getParameter("delay");
                    if (d != null) {
                        delay = Integer.parseInt(d);
                    }
                } catch (Exception e) {
                    // ignore if it's not a number
                }

                /* initialize response */
                response.setHeader("Content-Type", "text/event-stream;charset=UTF-8");
                response.setHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
                response.setHeader("Pragma", "no-cache");

                logger.info("Starting poller");

                // we will use a "single-writer" approach where the Servlet thread does all the writing
                // by fetching JSON messages from the MetricJsonListener to write them to the output
                try {
                    final PrintWriter writer = response.getWriter();
                    while (!isDestroyed) {
                        writeDataTo(writer);
                        
                        /* shortcut breaking out of loop if we have been destroyed */
                        if(isDestroyed) {
                            break;
                        }
                        
                        // after outputting all the messages we will flush the stream
                        response.flushBuffer();
                        
                        // explicitly check for client disconnect - PrintWriter does not throw exceptions
                        if (response.getWriter().checkError()) {
                        	throw new IOException("io error");
                        }

                        // now wait the 'delay' time
                        Thread.sleep(delay);
                    }
                } catch (InterruptedException e) {
                    logger.debug("InterruptedException. Will stop polling.");	
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    // debug instead of error as we expect to get these whenever a client disconnects or network issue occurs
                    logger.debug("IOException while trying to write (generally caused by client disconnecting). Will stop polling.", e);
                } catch (Exception e) {
                    logger.error("Failed to write. Will stop polling.", e);
                }
                logger.debug("Stopping Turbine stream to connection");
            }
        } catch (Exception e) {
            logger.error("Error initializing servlet for metrics event stream.", e);
        } finally {
            concurrentConnections.decrementAndGet();
        }
    }

    private static void writeDataTo(Writer writer) throws IOException {
        boolean output = false;
        for (HystrixCommandMetrics commandMetrics : HystrixCommandMetrics.getInstances()) {
            output = true;
            writer.write("data: ");
            writeCommandData(commandMetrics, writer);
            writer.write("\n");
        }

        for (HystrixThreadPoolMetrics threadPoolMetrics : HystrixThreadPoolMetrics.getInstances()) {
            if (hasExecutedCommandsOnThread(threadPoolMetrics)) {
                output = true;
                writer.write("data: ");
                writeThreadPoolData(threadPoolMetrics, writer);
                writer.write("\n");
            }
        }

        for (HystrixCollapserMetrics collapserMetrics : HystrixCollapserMetrics.getInstances()) {
            output = true;
            writer.write("data: ");
            writerCollapserData(collapserMetrics, writer);
            writer.write("\n");
        }

        if (!output) {
            // https://github.com/Netflix/Hystrix/issues/85 hystrix.stream holds connection open if no metrics
            // we send a ping to test the connection so that we'll get an IOException if the client has disconnected
            writer.write("ping: \n");
        }
    }

    private static void writeCommandData(HystrixCommandMetrics commandMetrics, Writer writer) throws IOException {
        HystrixCommandKey key = commandMetrics.getCommandKey();
        HystrixCircuitBreaker circuitBreaker = HystrixCircuitBreaker.Factory.getInstance(key);

        JsonGenerator json = jsonFactory.createGenerator(writer);

        json.writeStartObject();
        json.writeStringField("type", "HystrixCommand");
        json.writeStringField("name", key.name());
        json.writeStringField("group", commandMetrics.getCommandGroup().name());
        json.writeNumberField("currentTime", System.currentTimeMillis());

        // circuit breaker
        if (circuitBreaker == null) {
            // circuit breaker is disabled and thus never open
            json.writeBooleanField("isCircuitBreakerOpen", false);
        } else {
            json.writeBooleanField("isCircuitBreakerOpen", circuitBreaker.isOpen());
        }
        HystrixCommandMetrics.HealthCounts healthCounts = commandMetrics.getHealthCounts();
        json.writeNumberField("errorPercentage", healthCounts.getErrorPercentage());
        json.writeNumberField("errorCount", healthCounts.getErrorCount());
        json.writeNumberField("requestCount", healthCounts.getTotalRequests());

        // rolling counters
        json.writeNumberField("rollingCountBadRequests", commandMetrics.getRollingCount(HystrixRollingNumberEvent.BAD_REQUEST));
        json.writeNumberField("rollingCountCollapsedRequests", commandMetrics.getRollingCount(HystrixRollingNumberEvent.COLLAPSED));
        json.writeNumberField("rollingCountEmit", commandMetrics.getRollingCount(HystrixRollingNumberEvent.EMIT));
        json.writeNumberField("rollingCountExceptionsThrown", commandMetrics.getRollingCount(HystrixRollingNumberEvent.EXCEPTION_THROWN));
        json.writeNumberField("rollingCountFailure", commandMetrics.getRollingCount(HystrixRollingNumberEvent.FAILURE));
        json.writeNumberField("rollingCountEmit", commandMetrics.getRollingCount(HystrixRollingNumberEvent.FALLBACK_EMIT));
        json.writeNumberField("rollingCountFallbackFailure", commandMetrics.getRollingCount(HystrixRollingNumberEvent.FALLBACK_FAILURE));
        json.writeNumberField("rollingCountFallbackRejection", commandMetrics.getRollingCount(HystrixRollingNumberEvent.FALLBACK_REJECTION));
        json.writeNumberField("rollingCountFallbackSuccess", commandMetrics.getRollingCount(HystrixRollingNumberEvent.FALLBACK_SUCCESS));
        json.writeNumberField("rollingCountResponsesFromCache", commandMetrics.getRollingCount(HystrixRollingNumberEvent.RESPONSE_FROM_CACHE));
        json.writeNumberField("rollingCountSemaphoreRejected", commandMetrics.getRollingCount(HystrixRollingNumberEvent.SEMAPHORE_REJECTED));
        json.writeNumberField("rollingCountShortCircuited", commandMetrics.getRollingCount(HystrixRollingNumberEvent.SHORT_CIRCUITED));
        json.writeNumberField("rollingCountSuccess", commandMetrics.getRollingCount(HystrixRollingNumberEvent.SUCCESS));
        json.writeNumberField("rollingCountThreadPoolRejected", commandMetrics.getRollingCount(HystrixRollingNumberEvent.THREAD_POOL_REJECTED));
        json.writeNumberField("rollingCountTimeout", commandMetrics.getRollingCount(HystrixRollingNumberEvent.TIMEOUT));

        json.writeNumberField("currentConcurrentExecutionCount", commandMetrics.getCurrentConcurrentExecutionCount());
        json.writeNumberField("rollingMaxConcurrentExecutionCount", commandMetrics.getRollingMaxConcurrentExecutions());

        // latency percentiles
        json.writeNumberField("latencyExecute_mean", commandMetrics.getExecutionTimeMean());
        json.writeObjectFieldStart("latencyExecute");
        json.writeNumberField("0", commandMetrics.getExecutionTimePercentile(0));
        json.writeNumberField("25", commandMetrics.getExecutionTimePercentile(25));
        json.writeNumberField("50", commandMetrics.getExecutionTimePercentile(50));
        json.writeNumberField("75", commandMetrics.getExecutionTimePercentile(75));
        json.writeNumberField("90", commandMetrics.getExecutionTimePercentile(90));
        json.writeNumberField("95", commandMetrics.getExecutionTimePercentile(95));
        json.writeNumberField("99", commandMetrics.getExecutionTimePercentile(99));
        json.writeNumberField("99.5", commandMetrics.getExecutionTimePercentile(99.5));
        json.writeNumberField("100", commandMetrics.getExecutionTimePercentile(100));
        json.writeEndObject();
        //
        json.writeNumberField("latencyTotal_mean", commandMetrics.getTotalTimeMean());
        json.writeObjectFieldStart("latencyTotal");
        json.writeNumberField("0", commandMetrics.getTotalTimePercentile(0));
        json.writeNumberField("25", commandMetrics.getTotalTimePercentile(25));
        json.writeNumberField("50", commandMetrics.getTotalTimePercentile(50));
        json.writeNumberField("75", commandMetrics.getTotalTimePercentile(75));
        json.writeNumberField("90", commandMetrics.getTotalTimePercentile(90));
        json.writeNumberField("95", commandMetrics.getTotalTimePercentile(95));
        json.writeNumberField("99", commandMetrics.getTotalTimePercentile(99));
        json.writeNumberField("99.5", commandMetrics.getTotalTimePercentile(99.5));
        json.writeNumberField("100", commandMetrics.getTotalTimePercentile(100));
        json.writeEndObject();

        // property values for reporting what is actually seen by the command rather than what was set somewhere
        HystrixCommandProperties commandProperties = commandMetrics.getProperties();

        json.writeNumberField("propertyValue_circuitBreakerRequestVolumeThreshold", commandProperties.circuitBreakerRequestVolumeThreshold().get());
        json.writeNumberField("propertyValue_circuitBreakerSleepWindowInMilliseconds", commandProperties.circuitBreakerSleepWindowInMilliseconds().get());
        json.writeNumberField("propertyValue_circuitBreakerErrorThresholdPercentage", commandProperties.circuitBreakerErrorThresholdPercentage().get());
        json.writeBooleanField("propertyValue_circuitBreakerForceOpen", commandProperties.circuitBreakerForceOpen().get());
        json.writeBooleanField("propertyValue_circuitBreakerForceClosed", commandProperties.circuitBreakerForceClosed().get());
        json.writeBooleanField("propertyValue_circuitBreakerEnabled", commandProperties.circuitBreakerEnabled().get());

        json.writeStringField("propertyValue_executionIsolationStrategy", commandProperties.executionIsolationStrategy().get().name());
        json.writeNumberField("propertyValue_executionIsolationThreadTimeoutInMilliseconds", commandProperties.executionTimeoutInMilliseconds().get());
        json.writeNumberField("propertyValue_executionTimeoutInMilliseconds", commandProperties.executionTimeoutInMilliseconds().get());
        json.writeBooleanField("propertyValue_executionIsolationThreadInterruptOnTimeout", commandProperties.executionIsolationThreadInterruptOnTimeout().get());
        json.writeStringField("propertyValue_executionIsolationThreadPoolKeyOverride", commandProperties.executionIsolationThreadPoolKeyOverride().get());
        json.writeNumberField("propertyValue_executionIsolationSemaphoreMaxConcurrentRequests", commandProperties.executionIsolationSemaphoreMaxConcurrentRequests().get());
        json.writeNumberField("propertyValue_fallbackIsolationSemaphoreMaxConcurrentRequests", commandProperties.fallbackIsolationSemaphoreMaxConcurrentRequests().get());

                    /*
                     * The following are commented out as these rarely change and are verbose for streaming for something people don't change.
                     * We could perhaps allow a property or request argument to include these.
                     */

        //                    json.put("propertyValue_metricsRollingPercentileEnabled", commandProperties.metricsRollingPercentileEnabled().get());
        //                    json.put("propertyValue_metricsRollingPercentileBucketSize", commandProperties.metricsRollingPercentileBucketSize().get());
        //                    json.put("propertyValue_metricsRollingPercentileWindow", commandProperties.metricsRollingPercentileWindowInMilliseconds().get());
        //                    json.put("propertyValue_metricsRollingPercentileWindowBuckets", commandProperties.metricsRollingPercentileWindowBuckets().get());
        //                    json.put("propertyValue_metricsRollingStatisticalWindowBuckets", commandProperties.metricsRollingStatisticalWindowBuckets().get());
        json.writeNumberField("propertyValue_metricsRollingStatisticalWindowInMilliseconds", commandProperties.metricsRollingStatisticalWindowInMilliseconds().get());

        json.writeBooleanField("propertyValue_requestCacheEnabled", commandProperties.requestCacheEnabled().get());
        json.writeBooleanField("propertyValue_requestLogEnabled", commandProperties.requestLogEnabled().get());

        json.writeNumberField("reportingHosts", 1); // this will get summed across all instances in a cluster

        json.writeEndObject();
        json.close();
    }

    private static boolean hasExecutedCommandsOnThread(HystrixThreadPoolMetrics threadPoolMetrics) {
        return threadPoolMetrics.getCurrentCompletedTaskCount().intValue() > 0;
    }

    private static void writeThreadPoolData(HystrixThreadPoolMetrics threadPoolMetrics, Writer writer) throws IOException {
        HystrixThreadPoolKey key = threadPoolMetrics.getThreadPoolKey();
        JsonGenerator json = jsonFactory.createGenerator(writer);
        json.writeStartObject();

        json.writeStringField("type", "HystrixThreadPool");
        json.writeStringField("name", key.name());
        json.writeNumberField("currentTime", System.currentTimeMillis());

        json.writeNumberField("currentActiveCount", threadPoolMetrics.getCurrentActiveCount().intValue());
        json.writeNumberField("currentCompletedTaskCount", threadPoolMetrics.getCurrentCompletedTaskCount().longValue());
        json.writeNumberField("currentCorePoolSize", threadPoolMetrics.getCurrentCorePoolSize().intValue());
        json.writeNumberField("currentLargestPoolSize", threadPoolMetrics.getCurrentLargestPoolSize().intValue());
        json.writeNumberField("currentMaximumPoolSize", threadPoolMetrics.getCurrentMaximumPoolSize().intValue());
        json.writeNumberField("currentPoolSize", threadPoolMetrics.getCurrentPoolSize().intValue());
        json.writeNumberField("currentQueueSize", threadPoolMetrics.getCurrentQueueSize().intValue());
        json.writeNumberField("currentTaskCount", threadPoolMetrics.getCurrentTaskCount().longValue());
        json.writeNumberField("rollingCountThreadsExecuted", threadPoolMetrics.getRollingCount(HystrixRollingNumberEvent.THREAD_EXECUTION));
        json.writeNumberField("rollingMaxActiveThreads", threadPoolMetrics.getRollingMaxActiveThreads());
        json.writeNumberField("rollingCountCommandRejections", threadPoolMetrics.getRollingCount(HystrixRollingNumberEvent.THREAD_POOL_REJECTED));

        json.writeNumberField("propertyValue_queueSizeRejectionThreshold", threadPoolMetrics.getProperties().queueSizeRejectionThreshold().get());
        json.writeNumberField("propertyValue_metricsRollingStatisticalWindowInMilliseconds", threadPoolMetrics.getProperties().metricsRollingStatisticalWindowInMilliseconds().get());

        json.writeNumberField("reportingHosts", 1); // this will get summed across all instances in a cluster

        json.writeEndObject();
        json.close();
    }

    private static void writerCollapserData(HystrixCollapserMetrics collapserMetrics, Writer writer) throws IOException {
        HystrixCollapserKey key = collapserMetrics.getCollapserKey();
        JsonGenerator json = jsonFactory.createGenerator(writer);
        json.writeStartObject();

        json.writeStringField("type", "HystrixCollapser");
        json.writeStringField("name", key.name());
        json.writeNumberField("currentTime", System.currentTimeMillis());

        json.writeNumberField("rollingCountRequestsBatched", collapserMetrics.getRollingCount(HystrixRollingNumberEvent.COLLAPSER_REQUEST_BATCHED));
        json.writeNumberField("rollingCountBatches", collapserMetrics.getRollingCount(HystrixRollingNumberEvent.COLLAPSER_BATCH));
        json.writeNumberField("rollingCountResponsesFromCache", collapserMetrics.getRollingCount(HystrixRollingNumberEvent.RESPONSE_FROM_CACHE));

        // batch size percentiles
        json.writeNumberField("batchSize_mean", collapserMetrics.getBatchSizeMean());
        json.writeObjectFieldStart("batchSize");
        json.writeNumberField("25", collapserMetrics.getBatchSizePercentile(25));
        json.writeNumberField("50", collapserMetrics.getBatchSizePercentile(50));
        json.writeNumberField("75", collapserMetrics.getBatchSizePercentile(75));
        json.writeNumberField("90", collapserMetrics.getBatchSizePercentile(90));
        json.writeNumberField("95", collapserMetrics.getBatchSizePercentile(95));
        json.writeNumberField("99", collapserMetrics.getBatchSizePercentile(99));
        json.writeNumberField("99.5", collapserMetrics.getBatchSizePercentile(99.5));
        json.writeNumberField("100", collapserMetrics.getBatchSizePercentile(100));
        json.writeEndObject();

        // shard size percentiles (commented-out for now)
        //json.writeNumberField("shardSize_mean", collapserMetrics.getShardSizeMean());
        //json.writeObjectFieldStart("shardSize");
        //json.writeNumberField("25", collapserMetrics.getShardSizePercentile(25));
        //json.writeNumberField("50", collapserMetrics.getShardSizePercentile(50));
        //json.writeNumberField("75", collapserMetrics.getShardSizePercentile(75));
        //json.writeNumberField("90", collapserMetrics.getShardSizePercentile(90));
        //json.writeNumberField("95", collapserMetrics.getShardSizePercentile(95));
        //json.writeNumberField("99", collapserMetrics.getShardSizePercentile(99));
        //json.writeNumberField("99.5", collapserMetrics.getShardSizePercentile(99.5));
        //json.writeNumberField("100", collapserMetrics.getShardSizePercentile(100));
        //json.writeEndObject();

        //json.writeNumberField("propertyValue_metricsRollingStatisticalWindowInMilliseconds", collapserMetrics.getProperties().metricsRollingStatisticalWindowInMilliseconds().get());
        json.writeBooleanField("propertyValue_requestCacheEnabled", collapserMetrics.getProperties().requestCacheEnabled().get());
        json.writeNumberField("propertyValue_maxRequestsInBatch", collapserMetrics.getProperties().maxRequestsInBatch().get());
        json.writeNumberField("propertyValue_timerDelayInMilliseconds", collapserMetrics.getProperties().timerDelayInMilliseconds().get());

        json.writeNumberField("reportingHosts", 1); // this will get summed across all instances in a cluster

        json.writeEndObject();
        json.close();
    }
}
