# AWS execution

The production design uses:

- Amazon S3 for the shared input and output data;
- Amazon EMR for the PySpark implementation; and
- one Amazon EC2 instance for the Pandas comparison.

Exact EMR release, instance types, node counts, Spark partitions and S3 paths
will be recorded here after the AWS environment is created.

Never store access keys, secret keys, account identifiers or private key files
in this repository.

