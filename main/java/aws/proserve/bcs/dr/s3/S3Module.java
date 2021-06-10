// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.annotation.Default;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.s3.dto.Stream;
import aws.proserve.bcs.dr.secret.Credential;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.jmespath.ObjectMapperSingleton;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.Module;
import dagger.Provides;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

@Singleton
@Module
class S3Module {
    static final String KINESIS_APP = "DRPS3-KinesisApp-";

    @Default
    @Singleton
    @Provides
    static AmazonS3 defaultS3(
            @Nullable @Default String region,
            @Nullable Credential credential) {
        return AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(Credential.toProvider(credential))
                .build();
    }

    @Source
    @Singleton
    @Provides
    static AmazonS3 sourceS3(
            @Nullable @Source String region,
            @Nullable Credential credential) {
        return AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(Credential.toProvider(credential))
                .build();
    }

    @Target
    @Provides
    @Singleton
    static AmazonS3 targetS3(
            @Nullable @Target String region,
            @Nullable Credential credential) {
        return AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(Credential.toProvider(credential))
                .build();
    }

    @Source
    @Singleton
    @Provides
    static TransferManager sourceTransfer(@Nullable @Source AmazonS3 s3) {
        return TransferManagerBuilder.standard().withS3Client(s3).build();
    }

    @Target
    @Provides
    @Singleton
    static TransferManager targetTransfer(@Nullable @Target AmazonS3 s3) {
        return TransferManagerBuilder.standard().withS3Client(s3).build();
    }

    @Provides
    @Singleton
    ObjectMapper objectMapper() {
        return ObjectMapperSingleton.getObjectMapper();
    }

    @Provides
    @Singleton
    AWSSecretsManager secretsManager() {
        return AWSSecretsManagerClientBuilder.defaultClient();
    }

    /**
     * @apiNote this lambda function is deployed together with the step functions.
     */
    @Provides
    @Singleton
    @Default
    static AWSStepFunctions stepFunctions() {
        return AWSStepFunctionsClientBuilder.defaultClient();
    }

    /**
     * @apiNote Dynamo table is created by kinesis, which always runs at target region, credential is unnecessary.
     */
    @Singleton
    @Provides
    static DynamoDB dynamoDB(@Nullable @Target String region) {
        return new DynamoDB(AmazonDynamoDBClientBuilder.standard().withRegion(region).build());
    }

    @Provides
    @Singleton
    static Worker kinesisWorker(
            @Nullable @Stream String streamName,
            @Nullable @Target String region,
            RecordProcessorFactory processorFactory) {
        final String workerId;
        try {
            workerId = "DRPS3-KinesisWorker"
                    + "-" + InetAddress.getLocalHost().getCanonicalHostName()
                    + "-" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }

        final var rawName = KINESIS_APP + streamName;

        final var worker = new Worker.Builder()
                .recordProcessorFactory(processorFactory)
                .config(new KinesisClientLibConfiguration(
                        rawName.substring(0, Math.min(255, rawName.length())),
                        streamName,
                        new DefaultAWSCredentialsProviderChain(),
                        workerId)
                        .withRegionName(region)
                        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON))
                .build();
        processorFactory.setWorker(worker);
        return worker;
    }

    /**
     * @apiNote Kinesis always runs at target region, credential is unnecessary.
     */
    @Singleton
    @Provides
    static AmazonKinesis kinesis(@Nullable @Target String region) {
        return AmazonKinesisClientBuilder.standard().withRegion(region).build();
    }

    /**
     * @apiNote Kinesis always runs at target region, credential is unnecessary.
     */
    @Singleton
    @Provides
    static KinesisProducer producer(@Nullable @Target String region) {
        return new KinesisProducer(new KinesisProducerConfiguration().setRegion(region));
    }
}
