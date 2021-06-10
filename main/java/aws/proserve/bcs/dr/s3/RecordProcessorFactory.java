// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.annotation.Default;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.lambda.annotation.TaskToken;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Singleton
class RecordProcessorFactory implements IRecordProcessorFactory {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AmazonS3 sourceS3;
    private final AmazonS3 targetS3;
    private final ReplicateFile replicateFile;
    private final AWSStepFunctions stepFunctions;
    private final String taskToken;

    private Worker worker;

    @Inject
    RecordProcessorFactory(
            @Source AmazonS3 sourceS3,
            @Target AmazonS3 targetS3,
            @Nullable ReplicateFile replicateFile,
            @Default AWSStepFunctions stepFunctions,
            @Nullable @TaskToken String taskToken) {
        this.sourceS3 = sourceS3;
        this.targetS3 = targetS3;
        this.replicateFile = replicateFile;
        this.stepFunctions = stepFunctions;
        this.taskToken = taskToken;
    }

    void setWorker(Worker worker) {
        this.worker = worker;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new RecordProcessor(sourceS3, targetS3, replicateFile, stepFunctions, taskToken, shutdown());
    }

    /**
     * @apiNote Must run in a separate thread, otherwise the record processor cannot properly shutdown.
     * <p>
     * Must not return singleton shutdown runnable to the record processor. While one thread is waiting for shutdown
     * gracefully, the other thread may complete the whole shutdown and interrupt the waiting.
     */
    private Runnable shutdown() {
        return () -> {
            final var thread = new Thread(
                    () -> {
                        log.info("Start shutdown gracefully.");
                        final var shutdownFuture = worker.startGracefulShutdown();

                        // for copying very big files
                        log.info("Wait up to one day for shutdown to complete.");
                        try {
                            shutdownFuture.get(1, TimeUnit.DAYS);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            log.info("Unable to shutdown gracefully, thus shutdown directly.", e);
                            worker.shutdown();
                        }

                        log.info("Worker shutdown.");
                    });
            thread.setDaemon(true);
            thread.start();
        };
    }
}
