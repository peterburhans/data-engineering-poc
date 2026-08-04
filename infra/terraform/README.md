# Terraform infrastructure

The root module owns deployment composition: provider configuration, the global naming contract, environment inputs, cross-module wiring, and public outputs.

## Modules

- `data_lake` owns the raw and curated S3 buckets and their baseline security controls.
- `streaming` owns Kinesis Data Streams, Firehose, and its least-privilege delivery role.

Small root resources hold the Mooncake connection secret and the DynamoDB object-bookmark ledger used by local Glue containers. Terraform does not create LocalStack Glue API resources; Airflow runs AWS's Glue 5.0 image directly.

Modules are separated by lifecycle and responsibility. Small root concerns such as naming remain local rather than becoming one-line utility modules.

Physical names are calculated once in the root and passed into modules. Names use `{env}_{name}_{resource_shortcode}`. S3 bucket names use the equivalent hyphenated form because bucket DNS names cannot contain underscores.

## Validation

```bash
terraform fmt -check -recursive
terraform init
terraform validate
terraform plan
```

For LocalStack, Compose supplies the endpoint, disposable-bucket setting, environment, project name, and warehouse password variables.
