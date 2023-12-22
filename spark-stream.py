import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        postcode TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data_to_cassandra(session, **kwargs):
    print("Inserting data...")
    user_id = kwargs.get('id')
    first_name = kwargs.get('firstname')
    last_name = kwargs.get('lastname')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        if user_id is not None:
            session.execute("""
                INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                    postcode, email, username, dob, registered_date, phone, picture)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, first_name, last_name, gender, address,
                  postcode, email, username, dob, registered_date, phone, picture))
            logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        # To check cassandra connector & spark sql -> MVN Repository
        # ScalaVersion:PackageVersion
        # Save jar files in Lib\site-packages\pyspark\jars\
        s_conn = SparkSession.builder \
            .appName('RealTimeDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("firstname", StringType(), False),
        StructField("lastname", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
# FALTA AVERIGUAR POR QUE USER_ID ES NONE -> CÓMO TENGO QUE LLAMAR A LA FUNCIÓN INSERT_INTO()?

            logging.info("Streaming is being started...")

            streaming_query = (
                selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                .option('checkpointLocation', '/tmp/checkpoint')
                .option('keyspace', 'spark_streams')
                .option('table', 'created_users')
                .foreachBatch(lambda batch, batch_id:
                              batch.foreach(lambda row: insert_data_to_cassandra(session, row.asDict())))
                .start()
            )

            streaming_query.awaitTermination()
