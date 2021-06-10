# Disaster Recovery Factory - S3

## Introduction
This project is published as a solution in the Amazon Web Services Solutions Library.
For more information, including how to deploy it into your AWS account, please visit:
- https://www.amazonaws.cn/en/solutions/disaster-recovery-factory

### This Package
This package contains AWS Lambda handlers and AWS Fargate task to manage cross-regional replication of S3 buckets and objects.

To build the docker image for the replication, go to the project directory root and run
```bash
docker build \
--file DockerFile \
--tag drportal/s3/replicate-bucket:latest \
build/lambda
```

## AWS Blogs
The following blog articles introduce in depth how this solution works and how to make the most out of it.
- [Use Disaster Recovery Factory to efficiently manage instance disaster recovery configurations](https://aws.amazon.com/cn/blogs/china/use-cloud-disaster-recovery-management-tools-to-efficiently-manage-instance-disaster-recovery-configuration/) (March 2021)
- [Migrate and protect EC2 instances by Disaster Recovery Factory](https://aws.amazon.com/cn/blogs/china/gcr-blog-migrate-and-protect-ec2-instances-using-cloud-disaster-management-tools/) (July 2020)
