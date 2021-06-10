// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.BoolHandler;
import aws.proserve.bcs.dr.lambda.annotation.Default;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

public class CheckBucketValid implements BoolHandler<CheckBucketValid.Request> {

    @Override
    public boolean handleRequest(Request request, Context context) {
        return S3Component.build(request.getProjectId(), request.getBucket().getRegion())
                .checkBucketValid()
                .check(request.getBucket().getName());
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

        boolean check(String bucketName) {
            if (bucketName == null || bucketName.isEmpty() || bucketName.contains(".")) {
                log.warn("Invalid bucket name [{}], cannot contain '.'", bucketName);
                return false;
            }

            final var result = s3.listBuckets(new ListBucketsRequest());
            if (result.stream().map(Bucket::getName).noneMatch(bucketName::equals)) {
                log.warn("Unable to find bucket [{}]", bucketName);
                return false;
            }

            return true;
        }
    }
}
