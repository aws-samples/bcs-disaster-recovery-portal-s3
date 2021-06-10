// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.annotation.Default;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.lambda.annotation.TaskToken;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import aws.proserve.bcs.dr.s3.dto.Stream;
import aws.proserve.bcs.dr.secret.Credential;
import aws.proserve.bcs.dr.secret.SecretManager;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import dagger.BindsInstance;
import dagger.Component;

import javax.annotation.Nullable;
import javax.inject.Singleton;

@Singleton
@Component(modules = S3Module.class)
interface S3Component {

    static S3Component build(Resource resource) {
        return DaggerS3Component.builder()
                .stream(resource.getName())
                .targetRegion(resource.getRegion())
                .build();
    }

    static S3Component build(String projectId, String region) {
        return DaggerS3Component.builder()
                .region(region)
                .credential(projectId == null ? null :
                        DaggerS3Component.builder()
                                .build()
                                .secretManager()
                                .getCredentialByProject(projectId))
                .build();
    }

    static S3Component build(String projectId, String source, String target, String stream,
                             ReplicateBucket.Request request) {
        return DaggerS3Component.builder()
                .sourceRegion(source)
                .targetRegion(target)
                .stream(stream)
                .credential(projectId == null ? null :
                        DaggerS3Component.builder()
                                .build()
                                .secretManager()
                                .getCredentialByProject(projectId))
                .replicateRequest(request)
                .build();
    }

    SecretManager secretManager();

    @Default
    AWSStepFunctions stepFunctions();

    CheckBucketValid.Worker checkBucketValid();

    CreateStream.Worker createStream();

    DeleteStream.Worker deleteStream();

    DeleteDynamo.Worker deleteDynamo();

    ScanBucket.Worker scanBucket();

    SetBucketAccelerate.Worker setBucketAccelerate();

    Worker replicateWorker();

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder taskToken(@Nullable @TaskToken String taskToken);

        @BindsInstance
        Builder stream(@Nullable @Stream String stream);

        @BindsInstance
        Builder region(@Nullable @Default String region);

        @BindsInstance
        Builder sourceRegion(@Nullable @Source String region);

        @BindsInstance
        Builder targetRegion(@Nullable @Target String region);

        @BindsInstance
        Builder credential(@Nullable Credential credential);

        @BindsInstance
        Builder replicateRequest(@Nullable ReplicateBucket.Request request);

        S3Component build();
    }
}
