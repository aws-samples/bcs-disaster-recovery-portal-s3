// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.BoolHandler;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.AmazonKinesisException;
import com.amazonaws.services.lambda.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

public class DeleteStream implements BoolHandler<DeleteStream.Request> {

    @Override
    public boolean handleRequest(Request request, Context context) {
        return S3Component.build(request.getStream())
                .deleteStream()
                .delete(request.getStream().getName());
    }

    static class Request {
        private Resource stream;

        public Resource getStream() {
            return stream;
        }

        public void setStream(Resource stream) {
            this.stream = stream;
        }
    }

    @Singleton
    static class Worker {
        private final Logger log = LoggerFactory.getLogger(getClass());
        private final AmazonKinesis kinesis;

        @Inject
        Worker(AmazonKinesis kinesis) {
            this.kinesis = kinesis;
        }

        boolean delete(String name) {
            log.info("Delete a stream named [{}]", name);
            try {
                kinesis.deleteStream(name);
                return true;
            } catch (AmazonKinesisException e) {
                log.warn("Unable to delete stream [" + name + "]", e);
                return false;
            }
        }
    }
}
