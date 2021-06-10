// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.BoolHandler;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

public class DeleteDynamo implements BoolHandler<DeleteDynamo.Request> {

    @Override
    public boolean handleRequest(Request request, Context context) {
        return S3Component.build(request.getStream())
                .deleteDynamo()
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
        private final DynamoDB dynamoDB;

        @Inject
        Worker(DynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
        }

        boolean delete(String name) {
            final var tableName = S3Module.KINESIS_APP + name;
            final var table = dynamoDB.getTable(tableName);
            log.info("Delete a table named [{}]", tableName);
            try {
                table.delete();
                table.waitForDelete();
                return true;
            } catch (InterruptedException e) {
                log.warn("Unable to delete table [" + tableName + "]", e);
                return false;
            }
        }
    }
}
