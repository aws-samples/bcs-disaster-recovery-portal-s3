// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.StringHandler;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import aws.proserve.bcs.dr.lambda.util.Assure;
import aws.proserve.bcs.dr.util.Preconditions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.lambda.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class CreateStream implements StringHandler<CreateStream.Request> {

    @Override
    public String handleRequest(Request request, Context context) {
        return S3Component.build(request.getBucket())
                .createStream()
                .create(request.getBucket().getName());
    }

    static class Request {
        private Resource bucket;

        public Resource getBucket() {
            return bucket;
        }

        public void setBucket(Resource bucket) {
            this.bucket = bucket;
        }
    }

    @Singleton
    static class Worker {
        static final String INVALID_CHAR = "[^0-9a-zA-Z-_\\.]";

        private final Logger log = LoggerFactory.getLogger(getClass());
        private final AmazonKinesis kinesis;

        @Inject
        Worker(AmazonKinesis kinesis) {
            this.kinesis = kinesis;
        }

        String create(String bucketName) {
            final var pruned = bucketName.replaceAll(INVALID_CHAR, "-");
            final var rawName = "DRPS3-Stream"
                    + "-" + ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyMMdd-HHmmss-SSS"))
                    + "-" + pruned;
            final var name = rawName.substring(0, Math.min(128, rawName.length()));
            log.info("Create a stream named [{}]", name);
            kinesis.createStream(name, 10);

            Assure.assure(() ->
                    Preconditions.checkState(isActive(name), "Stream [" + name + "] is not ACTIVE yet."));
            return isActive(name) ? name : null;
        }

        private boolean isActive(String name) {
            return "ACTIVE".equals(kinesis.describeStream(name).getStreamDescription().getStreamStatus());
        }
    }
}
