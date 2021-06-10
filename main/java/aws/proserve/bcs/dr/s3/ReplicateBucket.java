// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3;

import aws.proserve.bcs.dr.lambda.VoidHandler;
import aws.proserve.bcs.dr.lambda.dto.Resource;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;

import java.security.Security;
import java.util.Objects;

public class ReplicateBucket implements VoidHandler<ReplicateBucket.Request> {

    /**
     * @apiNote entry point for fargate.
     */
    public static void main(String[] args) {
        new ReplicateBucket().handleRequest(getRequest(), null);
        System.exit(0);
    }

    private static Request getRequest() {
        final var request = new Request();
        final var source = new Resource();
        final var target = new Resource();
        final var stream = new Resource();

        source.setName(env("source_bucket"));
        source.setRegion(env("source_region"));
        target.setName(env("target_bucket"));
        target.setRegion(env("target_region"));
        stream.setName(env("stream_name"));
        stream.setRegion(env("stream_region"));
        request.setTaskToken(env("task_token"));

        request.setSource(source);
        request.setTarget(target);
        request.setStream(stream);
        request.setProjectId(System.getenv("project_id"));
        return request;
    }

    private static String env(String key) {
        return Objects.requireNonNull(System.getenv(key), key + " cannot be null.");
    }

    @Override
    public void handleRequest(Request request, Context context) {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        Security.setProperty("networkaddress.cache.ttl", "60");

        final var component = S3Component.build(
                request.getProjectId(),
                request.getSource().getRegion(),
                request.getTarget().getRegion(),
                request.getStream().getName(),
                request);
        component.replicateWorker().run();
        component.stepFunctions().sendTaskSuccess(new SendTaskSuccessRequest()
                .withTaskToken(request.getTaskToken())
                .withOutput("{}"));
    }

    static class Request {
        private Resource source;
        private Resource target;
        private Resource stream;
        private String projectId;
        private String taskToken;

        public Resource getSource() {
            return source;
        }

        public void setSource(Resource source) {
            this.source = source;
        }

        public Resource getTarget() {
            return target;
        }

        public void setTarget(Resource target) {
            this.target = target;
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

        public String getTaskToken() {
            return taskToken;
        }

        public void setTaskToken(String taskToken) {
            this.taskToken = taskToken;
        }
    }
}
