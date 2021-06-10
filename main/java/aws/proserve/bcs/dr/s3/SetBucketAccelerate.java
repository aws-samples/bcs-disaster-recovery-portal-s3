// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.BoolHandler;
import aws.proserve.bcs.dr.lambda.annotation.Default;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import aws.proserve.bcs.dr.lambda.util.Assure;
import aws.proserve.bcs.dr.util.Preconditions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketAccelerateStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

public class SetBucketAccelerate implements BoolHandler<SetBucketAccelerate.Request> {

    @Override
    public boolean handleRequest(Request request, Context context) {
        return S3Component.build(request.getProjectId(), request.getBucket().getRegion())
                .setBucketAccelerate()
                .set(request.getBucket().getName());
    }

    static class Request {
        private Resource bucket;
        private String projectId;

        public Resource getBucket() {
            return bucket;
        }

        public void setBucket(Resource bucket) {
            this.bucket = bucket;
        }

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }
    }

    @Singleton
    static class Worker {
        private final Logger log = LoggerFactory.getLogger(getClass());
        private final AmazonS3 s3;

        @Inject
        Worker(@Default AmazonS3 s3) {
            this.s3 = s3;
        }

        boolean set(String bucketName) {
            try {
                if (isEnabled(bucketName)) {
                    return true;
                }
            } catch (AmazonS3Exception e) {
                log.warn("Unable to check the bucketAccelerate property for " + bucketName, e);
                return false;
            }

            log.info("Set bucketAccelerate for {}", bucketName);
            s3.setBucketAccelerateConfiguration(bucketName,
                    new BucketAccelerateConfiguration(BucketAccelerateStatus.Enabled));

            Assure.assure(() ->
                    Preconditions.checkState(isEnabled(bucketName), "BucketAccelerate is not enabled for " + bucketName));
            return isEnabled(bucketName);
        }

        private boolean isEnabled(String name) {
            return "Enabled".equals(s3.getBucketAccelerateConfiguration(name).getStatus());
        }
    }
}
