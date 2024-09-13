from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import pyspark.sql.functions as F


TOPIC = 'wikimedia_events'
BOOTSTRAP_SERVERS = 'confluent-local-broker-1:51383'

def console_output(df):
    query = df.writeStream.outputMode('complete').format('console').start()
    
def main():
    spark = SparkSession.builder \
        .appName("Wikimedia Kafka Consumer") \
        .getOrCreate()
    kafka_stream_df = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS)
        .option('subscribe', TOPIC)
        .load()
    )

    schema = StructType([
        StructField('timestamp', IntegerType()),
        StructField('bot', BooleanType()),
        StructField('minor', BooleanType()),
        StructField('user', StringType()),
        StructField('meta', StructType([
            StructField('domain', StringType())
        ])),
        StructField('length', StructType([
            StructField('old', IntegerType()),
            StructField('new', IntegerType())
        ]))
    ])

    df = kafka_stream_df.select(F.col('value').cast('string'))
    df = df.select(F.from_json(df.value, schema).alias('data'))
    df = df.select(
        'data.timestamp',
        'data.bot',
        'data.minor',
        'data.user',
        'data.meta.domain',
        F.col('data.length.old').alias('old_length'),
        F.col('data.length.new').alias('new_length')
    )
    df = df.withColumn('length_diff', F.col('new_length') - F.col('old_length'))
    df = df.withColumn('length_diff_percent', F.col('length_diff') / F.col('old_length') * 100)

    top_5_domains = df.groupBy('domain').count().orderBy(F.desc('count')).limit(5)
    console_output(top_5_domains)
    top_5_users = df.groupBy('user').agg(F.sum('length_diff').alias('length_diff_sum')).orderBy(F.desc('length_diff_sum')).limit(5)
    console_output(top_5_users)

    summary = df.agg(
        F.count('timestamp').alias('total_count'),
        (F.count_if(F.col('bot') == True) / F.count('bot')).alias('bot_percent'),
        F.mean('length_diff').alias('average_length_diff'),
        F.min('length_diff').alias('min_length_diff'),
        F.max('length_diff').alias('max_length_diff')
    )
    console_output(summary)
    (
        df.writeStream
        .outputMode('append')
        .option('checkpointLocation', 'output')
        .format('csv')
        .option('path', 'output/wikimedia_events.csv')
        .option('header', True)
        .trigger(processingTime='10 seconds')
        .start()
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()