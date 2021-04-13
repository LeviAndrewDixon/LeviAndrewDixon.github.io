---
layout: post
title:  "Integrate Spark with Glue metastore"
date:   2021-04-11 21:45:39 -0400
tags: aws emr spark glue hive data_lake
categories: emr spark glue
---
|Date|
--
|2021-04-11|

# Configure Spark, Hive, and Presto to transparently update the AWS Glue Catalog on EMR

This article provides information on configuring ([Hive][link-emr-hive], [Spark][link-emr-spark], and [Presto][link-emr-presto] (running on [EMR][link-emr]) to use the [AWS Glue Catalog][link-glue-components] to track Data Lake metadata vs. the [Hive Metastore][link-metastore], by overriding configuration properties that determine which Java classes are used to wrap [Metastore][link-metastore] access.  Using this method (vs. relying on the Glue Crawler) provides real time updates and is the most efficient way to keep the data up to date.

## What is the Hive Metastore ?

Hive tracks metadata describing database objects in an external relational database ([PostgreSQL][link-postgresql] or [MySQL][link-mysql]) called the [Metastore][link-metastore].   It maintains the following metadata:

- Database / schema definitions
  - name
  - FS location
- Table definitions
  - schema definition
  - storage details
  - partitions
  - properties
  - statistics
  - PK / FK constraints
  - FS location

## Glue vs. Hive Meta - Pros and Cons

Using the [Glue][link-glue-components] catalog provides a more central place in which to record and maintain the metadata of your Data Lake database objects (databases, tables, partitions, etc.) than [Hive Metastore][link-metastore]. There are pros and cons to both, but if you want to use other AWS services, you'll probably want to use the Glue Catalog.

### Pros
- Integration with other AWS Services such as [Lake Formation][link-lake-formation], [Athena][link-athena], [Redshift Spectrum][link-redshift-spectrum], [QuickSight][link-quick-sight]
- Performant
- Very little administration
- Low cost

### Cons
- Some Hive features are not supported with Glue
  - PK / FK constraints
  - Transactions
  - Indexes

## Configuration

- Hive (Property file: hive-site.xml, CloudFormation classification: hive-site)


    <property>hive.metastore.client.factory.class</property>
    <value>com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory<value>
    <property>hive.metastore.schema.verification</property>
    <value>false</value>
- Spark

    <property>hive.metastore.client.factory.class</property>
    <value>com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory<value>
    <property>hive.metastore.schema.verification</property>
    <value>false</value>

- Presto

Or via CloudFormation:

    {
      "Classification": "hive-site",
      "Properties": {
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "hive.metastore.schema.verification": "false"
      }
    }
    {
      "Classification": "spark-hive-site",
      "ConfigurationProperties": {
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        "hive.metastore.schema.verification": "false"
    },
    {
      "Classification": "presto-connector-hive",
      "ConfigurationProperties": {
        "hive.metastore": "glue"
        }
    },

## Application

Spark SQL queries will update the Glue Catalog instead of the Hive Metastore.

    spark-sql --num-executors=1 -e 'CREATE TABLE test.glue_test (id integer) stored as parquet'

Pyspark or Spark programs will update the Glue catalog instead of the Hive Metastore.

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

## Alternatives

1.  Using the configuration method with makes all operations transparently update the Glue catalog.  Alternatively, you can use the [GlueContext][https://docs.aws.amazon.com/glue/latest/dg/update-from-job.html] to update the catalog from a Glue or Spark job.
1. Use the Glue Crawler to crawl the file system to INFER the schema / structure and detect new partitions .  This is the least desirable option.  Schemas derived by infernal are likely to contain improperly assigned data types, which have to be corrected at a later date.  There is a delay between when the data is updated and when the catalog becomes aware of the change.  Scanning the data to discover schema changes is expensive, while updating the metadata as changes happen from the system where they occur is mostly "free".

## References

- [Using the AWS Glue Catalog as the Metastore for Hive][https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive-metastore-glue.html]

[link-postgresql]: https://www.postgresql.org/
[link-mysql]: https://dev.mysql.com/
[link-metastore]: https://cwiki.apache.org/confluence/display/Hive/Design
[link-glue-components]: https://docs.aws.amazon.com/glue/latest/dg/components-overview.html
[link-emr]: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html
[link-emr-hive]: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive.html
[link-emr-spark]: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html
[link-emr-presto]: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-presto.html
[link-athena]: https://docs.aws.amazon.com/athena/latest/ug/what-is.html
[link-cloud-formation]: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html
[link-lake-formation]: https://docs.aws.amazon.com/lake-formation/latest/dg/what-is-lake-formation.html
[link-quick-sight]: https://docs.aws.amazon.com/quicksight/latest/user/welcome.html
[link-redshit-spectrum]: https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum.html
