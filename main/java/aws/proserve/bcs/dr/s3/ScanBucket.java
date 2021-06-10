// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.VoidHandler;
import aws.proserve.bcs.dr.lambda.annotation.Source;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import aws.proserve.bcs.dr.s3.dto.ImmutableS3Object;
import aws.proserve.bcs.dr.s3.dto.S3Object;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;

public class ScanBucket implements VoidHandler<ScanBucket.Request> {

    @Override
    public void handleRequest(Request request, Context context) {
        S3Component.build(request.getProjectId(),
                request.getBucket().getRegion(),
                request.getStream().getRegion(),
                null,
                null)
                .scanBucket().scan(request);
    }

    static class Request {
        private Resource bucket;
        private Resource stream;
        private String projectId;

        public Resource getBucket() {
            return bucket;
        }

        public void setBucket(Resource bucket) {
            this.bucket = bucket;
        }

        public Resource getStream() {
            return stream;
        }

        public void setStream(Resource stream) {
            this.stream = stream;
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
        private final ObjectMapper mapper;
        private final AmazonS3 s3;
        private final KinesisProducer kinesis;

        @Inject
        Worker(ObjectMapper mapper, @Source AmazonS3 s3, KinesisProducer kinesis) {
            this.mapper = mapper;
            this.s3 = s3;
            this.kinesis = kinesis;
        }

        void scan(Request request) {
            final var objectSummaries = s3.listObjectsV2(request.getBucket().getName()).getObjectSummaries();
            var count = 0;
            for (var object : objectSummaries) {
                kinesis.addUserRecord(request.getStream().getName(),
                        object.getKey(), wrap(object.getKey(), object.getSize()));
                count++;
            }
            kinesis.addUserRecord(request.getStream().getName(),
                    S3Object.COMPLETED_KEY, wrap(S3Object.COMPLETED_KEY, S3Object.COMPLETED_SIZE));
            kinesis.flushSync();
            log.info("Scanned {} objects", count);
        }

        private ByteBuffer wrap(String key, long size) {
            try {
                return ByteBuffer.wrap(mapper.writeValueAsBytes(
                        ImmutableS3Object.builder().key(key).size(size).build()));
            } catch (JsonProcessingException e) {
                log.warn("Unable to wrap object", e);
                return null;
            }
        }
    }
}
