// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.annotation.Default;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.annotation.Target;
import aws.proserve.bcs.dr.lambda.annotation.TaskToken;
import aws.proserve.bcs.dr.s3.dto.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskFailureRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;

class ReplicateFile {
    private static final long ONE_KB = 1024;
    private static final long ONE_MB = 1024 * ONE_KB;
    private static final long TEN_MB = 10 * ONE_MB;
    private static final long _100_MB = 100 * ONE_MB;
    private static final long ONE_GB = 1024 * ONE_MB;
    private static final long ONE_TB = 1024 * ONE_GB;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String token;
    private final AmazonS3 sourceS3;
    private final AmazonS3 targetS3;
    private final TransferManager sourceManager;
    private final TransferManager targetManager;
    private final AWSStepFunctions targetSteps;
    private final ReplicateBucket.Request request;

    @Inject
    ReplicateFile(@Nullable @TaskToken String token,
                  @Source AmazonS3 sourceS3,
                  @Target AmazonS3 targetS3,
                  @Source TransferManager sourceManager,
                  @Target TransferManager targetManager,
                  @Default AWSStepFunctions targetSteps,
                  @Nullable ReplicateBucket.Request request) {
        this.token = token;
        this.sourceS3 = sourceS3;
        this.targetS3 = targetS3;
        this.sourceManager = sourceManager;
        this.targetManager = targetManager;
        this.targetSteps = targetSteps;
        this.request = request;
    }

    void copy(S3Object object) {
        copyX(object);
    }

    /**
     * @apiNote In-partition copy could be done by {@code TransferManager::copy} directly.
     */
    private void copyIn() {
        throw new IllegalStateException("Use TransferManager instead");
    }

    /**
     * Cross-partition copy, by downloading followed by uploading.
     */
    private void copyX(S3Object object) {
        final long size = object.getSize();
        if (size < _100_MB) {
            copyInMemoryX(object);
        } else if (size < ONE_TB) {
            copyByDiskX(object);
        } else {
            log.warn("Skip file {} as it is larger than 1 TB.", object.getKey());
        }
    }

    private void copyInMemoryX(S3Object object) {
        try {
            final var source = sourceS3.getObject(request.getSource().getName(), object.getKey());
            targetS3.putObject(request.getTarget().getName(), object.getKey(),
                    source.getObjectContent(), source.getObjectMetadata());
            log.info("Transferred {} via memory", object.getKey());
        } catch (AmazonS3Exception e) {
            final var cause = String.format("Unable to copy file [%s/%s] (%d) from [%s] to [%s] via memory.",
                    request.getSource().getName(), object.getKey(), object.getSize(),
                    request.getSource().getRegion(), request.getTarget().getRegion());
            log.warn(cause, e);

            if (token != null) {
                targetSteps.sendTaskFailure(new SendTaskFailureRequest()
                        .withTaskToken(token)
                        .withError(e.getClass().getSimpleName())
                        .withCause(cause + e.getMessage()));
            }
        }
    }

    /**
     * Downloads file one chunk at a time and uses multi-upload to complete uploading.
     */
    private void copyByDiskX(S3Object object) {
        final var upload = targetS3.initiateMultipartUpload(
                new InitiateMultipartUploadRequest(request.getTarget().getName(), object.getKey()));
        final var length = sourceS3.getObjectMetadata(request.getSource().getName(), object.getKey())
                .getContentLength();

        final File tmpFile;
        try {
            tmpFile = File.createTempFile(object.getKey(), ".tmp");
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create temporary file", e);
        }

        final var eTags = new ArrayList<PartETag>();
        var buffer = Math.max(TEN_MB, length / 1000);
        long position = 0;
        for (int i = 1; position < length; i++) {
            try {
                log.warn("Multipart [{}]: i {}, pos {}, buffer {}", object.getKey(), i, position, buffer);
                final var download = sourceManager.download(
                        new GetObjectRequest(request.getSource().getName(), object.getKey())
                                .withRange(position, Math.min(length, position + buffer)),
                        tmpFile);
                download.waitForCompletion();

                buffer = Math.min(buffer, length - position);
                final var result = targetS3.uploadPart(
                        new UploadPartRequest()
                                .withPartNumber(i)
                                .withUploadId(upload.getUploadId())
                                .withBucketName(request.getTarget().getName())
                                .withKey(object.getKey())
                                .withFileOffset(0)
                                .withFile(tmpFile)
                                .withPartSize(buffer));

                eTags.add(result.getPartETag());
                position += buffer;
            } catch (AmazonS3Exception | InterruptedException e) {
                final var cause = String.format("Unable to copy file [%s/%s] (%d) from [%s] to [%s] via disk.",
                        request.getSource().getName(), object.getKey(), object.getSize(),
                        request.getSource().getRegion(), request.getTarget().getRegion());
                log.warn("Multipart [{}]: i {}, pos {}, buffer {}", object.getKey(), i, position, buffer);
                log.warn(cause, e);

                final var uploads = targetS3.listMultipartUploads(
                        new ListMultipartUploadsRequest(request.getTarget().getName())).getMultipartUploads();
                log.warn("Abort {} multipart uploads", uploads.size());
                for (var u : uploads) {
                    targetS3.abortMultipartUpload(new AbortMultipartUploadRequest(
                            request.getTarget().getName(), u.getKey(), u.getUploadId()));
                }

                if (token != null) {
                    targetSteps.sendTaskFailure(new SendTaskFailureRequest()
                            .withTaskToken(token)
                            .withError(e.getClass().getSimpleName())
                            .withCause(cause + e.getMessage()));
                }

                tmpFile.delete();
                return;
            }
        }

        targetS3.completeMultipartUpload(new CompleteMultipartUploadRequest(
                request.getTarget().getName(), object.getKey(), upload.getUploadId(), eTags));
        tmpFile.delete();
    }
}
