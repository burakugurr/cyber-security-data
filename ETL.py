
import findspark
findspark.init()


import pyspark
from pyspark.sql import SparkSession
import os


url = "http://192.168.89.83:19120/api/v1"
# Nessie tablolarının tutulacağı bucket
full_path_to_warehouse = 's3a://warehouse'
# Bessie için kullanacağımız branch
ref = "main"
# Nessie authentication türü. Diğer seçenekler (NONE, BEARER, OAUTH2 or AWS)
auth_type = "NONE"
# AWS S3 yerine MinIO kullandığımız için. Spark'a amazona gitme burada kal demek için.
s3_endpoint = "http://192.168.89.83:9000"
# MinIO'ya erişim için. Bunlar root olarak docker-compose içinde belirtiliyor. Bu haliyle canlı ortamlarda kullanılmamalıdır.
accessKeyId='drYvJmGewawyxvrZnUqr'
secretAccessKey='c1O1FXQrUbmqOQmVzN5M7JaujU8S2EEEl3NbXjPZ'



spark = (
    SparkSession.builder
    .master("local")
    .appName("Spark Nessie Iceberg Demo")
    .config("spark.driver.memory", "16g")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .config('spark.jars.packages','org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.96.1')
    .config("spark.hadoop.fs.s3a.access.key", accessKeyId)
    .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey)
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Spark Amazon S3 varsayılan API'sine değil lokaldeki MinIO'ya gitsin.
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    # Spark extensions arasından Iceberg ve Nessie
    # Spark Nessie'yi nerede bulacak onun adresi
    .config("spark.sql.catalog.nessie.uri", url)
    # Hangi branch ile çalışacak
    .config("spark.sql.catalog.nessie.ref", ref)
    # Nessie'ye her gelen birşey sormasın. Hangi auth yöntemi ile sorulacak. Burada yok.
    .config("spark.sql.catalog.nessie.authentication.type", auth_type)
    # Katalog nessie olsun.
     .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    # Spark katalog implementasyonu iceberg.nessie olsun. Varsayılan kendi lokali
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    # Varsayılan warehouse adresini minio s3 warehouse bucket gösteriyoruz.
    .config("spark.sql.catalog.nessie.warehouse", full_path_to_warehouse)
    .config("fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)


from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType,StringType, FloatType


schema = StructType([
    StructField("TS", FloatType(), True),
    StructField("flow_duration", FloatType()),
    StructField("Header_Length", IntegerType()),
        StructField("DestinationIP" ,StringType()),
        StructField("SourceIP", StringType()),
        StructField("SourcePort",IntegerType()),
        StructField("DestinationPort",IntegerType()),
        StructField("ProtocolType",IntegerType()),
        StructField("Protocol_name" ,StringType()),
        StructField("Duration" ,StringType()),
        StructField("Rate",FloatType()),
        StructField("Srate",FloatType()),
        StructField("Drate",FloatType()),
        StructField("fin_flag_number",IntegerType()),
        StructField("syn_flag_number",IntegerType()),
        StructField("rst_flag_number",IntegerType()),
        StructField("psh_flag_number",IntegerType()),
        StructField("ack_flag_number",IntegerType()),
        StructField("urg_flag_number",IntegerType()),
        StructField("ece_flag_number",IntegerType()),
        StructField("cwr_flag_number",IntegerType()),
        StructField("ack_count",IntegerType()),
        StructField("syn_count",IntegerType()),
        StructField("fin_count",IntegerType()),
        StructField("urg_count",IntegerType()),
        StructField("rst_count",IntegerType()),
        StructField("max_duration",FloatType()),
        StructField("min_duration",FloatType()),
        StructField("sum_duration",FloatType()),
        StructField("average_duration",FloatType()),
        StructField("std_duration",FloatType()),
        StructField("CoAP",IntegerType()),
        StructField("HTTP",IntegerType()),
        StructField("HTTPS",IntegerType()),
        StructField("DNS",IntegerType()),
        StructField("Telnet",IntegerType()),
        StructField("SMTP",IntegerType()),
        StructField("SSH",IntegerType()),
        StructField("IRC",IntegerType()),
        StructField("TCP",IntegerType()),
        StructField("UDP",IntegerType()),
        StructField("DHCP",IntegerType()),
        StructField("ARP",IntegerType()),
        StructField("ICMP",IntegerType()),
        StructField("IGMP",IntegerType()),
        StructField("IPv",IntegerType()),
        StructField("LLC",IntegerType()),
        StructField("Tot_sum",FloatType()),
        StructField("Minn",FloatType()),
        StructField("Maxx",FloatType()),
        StructField("AVGG",FloatType()),
        StructField("Std",FloatType()),
        StructField("Totsize" ,StringType()),
        StructField("IAT",FloatType()),
        StructField("Number",IntegerType()),
        StructField("MAC",IntegerType()),
        StructField("Magnitue",FloatType()),
        StructField("Radius",FloatType()),
        StructField("Covariance",FloatType()),
        StructField("Variance",FloatType()),
        StructField("Weight",IntegerType()),
        StructField("DSstatus", StringType()),
        StructField("Fragments", StringType()),
        StructField("Sequencenumber", StringType()),
        StructField("ProtocolVersion",StringType()),
        StructField("flow_idle_time",FloatType()),
        StructField("flow_active_time",FloatType()),
        StructField("label",IntegerType()),
        StructField("subLabel",IntegerType()),
        StructField("subLabelCat",IntegerType()),

])


df = spark.read.csv("s3a://datasets/phase1_NetworkData.csv",header=True, schema=schema, inferSchema=True)
#df = spark.read.csv("s3a://datasets/phase1_NetworkData.csv",header=True,)

df.show(5)


from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType


df = df.withColumn('TS', col('TS').cast(TimestampType()))


from pyspark.sql.functions import to_date, from_unixtime,date_format, col, sum
from pyspark.sql.types import DateType,BooleanType

df = df\
    .withColumn("fin_flag_number", col("fin_flag_number").cast(BooleanType())) \
    .withColumn("syn_flag_number", col("syn_flag_number").cast(BooleanType()))\
    .withColumn("rst_flag_number", col("rst_flag_number").cast(BooleanType()))\
    .withColumn("psh_flag_number", col("psh_flag_number").cast(BooleanType()))\
    .withColumn("ack_flag_number", col("ack_flag_number").cast(BooleanType()))\
    .withColumn('urg_flag_number', col("urg_flag_number").cast(BooleanType()))\
    .withColumn('ece_flag_number', col("ece_flag_number").cast(BooleanType()))\
    .withColumn('cwr_flag_number', col("cwr_flag_number").cast(BooleanType()))\
    .withColumn('CoAP', col("CoAP").cast(BooleanType()))\
    .withColumn('HTTP', col("HTTP").cast(BooleanType()))\
    .withColumn('HTTPS', col("HTTPS").cast(BooleanType()))\
    .withColumn('DNS', col("DNS").cast(BooleanType()))\
    .withColumn('Telnet', col("Telnet").cast(BooleanType()))\
    .withColumn('SMTP', col("SMTP").cast(BooleanType()))\
    .withColumn('SSH', col("SSH").cast(BooleanType()))\
    .withColumn('IRC', col("IRC").cast(BooleanType()))\
    .withColumn('TCP', col("TCP").cast(BooleanType()))\
    .withColumn('UDP', col("UDP").cast(BooleanType()))\
    .withColumn('DHCP', col("DHCP").cast(BooleanType()))\
    .withColumn('ARP', col("ARP").cast(BooleanType()))\
    .withColumn('ICMP', col("ICMP").cast(BooleanType()))\
    .withColumn('IGMP', col("IGMP").cast(BooleanType()))\
    .withColumn('IPv', col("IPv").cast(BooleanType()))\
    .withColumn('LLC', col("LLC").cast(BooleanType()))\
    .withColumn('label', col("label").cast(BooleanType()))\
    .withColumn('subLabel', col("subLabel").cast(BooleanType()))\
    .withColumn('subLabelCat', col("subLabelCat").cast(BooleanType()))

df.show()



from functools import reduce

oldColumns = df.schema.names
newColumns = list()
for i in "TS, flow_duration, Header_Length, DestinationIP, SourceIP, SourcePort, DestinationPort, ProtocolType, Protocol_name, Duration, Rate, Srate, Drate, fin_flag_number, syn_flag_number, rst_flag_number, psh_flag_number, ack_flag_number, urg_flag_number, ece_flag_number, cwr_flag_number, ack_count, syn_count, fin_count, urg_count, rst_count, max_duration, min_duration, sum_duration, average_duration, std_duration, CoAP, HTTP, HTTPS, DNS, Telnet, SMTP, SSH, IRC, TCP, UDP, DHCP, ARP, ICMP, IGMP, IPv, LLC, Tot_sum, Minn, Maxx, AVGG, Std, Totsize, IAT, Number, MAC, Magnitue, Radius, Covariance, Variance, Weight, DSstatus, Fragments, Sequencenumber, ProtocolVersion, flow_idle_time, flow_active_time, label, subLabel, subLabelCat".split(", "):
    newColumns.append(i)

df_renamed = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)
df_renamed.show()


latest_df_schema = df_renamed.schema
latest_df_schema


spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.attackdb;")

spark.sql("DROP TABLE IF EXISTS nessie.attackdb.attackphase1;")

spark.sql("""
CREATE TABLE nessie.attackdb.attackphase1 (
TS timestamp,\
flow_duration float,\
Header_Length integer,\
DestinationIP string,\
SourceIP string,\
SourcePort integer,\
DestinationPort integer,\
ProtocolType integer,\
Protocol_name string,\
Duration string,\
Rate float,\
Srate float,\
Drate float,\
fin_flag_number boolean,\
syn_flag_number boolean,\
rst_flag_number boolean,\
psh_flag_number boolean,\
ack_flag_number boolean,\
urg_flag_number boolean,\
ece_flag_number boolean,\
cwr_flag_number boolean,\
ack_count integer,\
syn_count integer,\
fin_count integer,\
urg_count integer,\
rst_count integer,\
max_duration float,\
min_duration float,\
sum_duration float,\
average_duration float,\
std_duration float,\
CoAP boolean,\
HTTP boolean,\
HTTPS boolean,\
DNS boolean,\
Telnet boolean,\
SMTP boolean,\
SSH boolean,\
IRC boolean,\
TCP boolean,\
UDP boolean,\
DHCP boolean,\
ARP boolean,\
ICMP boolean,\
IGMP boolean,\
IPv boolean,\
LLC boolean,\
Tot_sum float,\
Minn float,\
Maxx float,\
AVGG float,\
Std float,\
Totsize string,\
IAT float,\
Number integer,\
MAC integer,\
Magnitue float,\
Radius float,\
Covariance float,\
Variance float,\
Weight integer,\
DSstatus string,\
Fragments string,\
Sequencenumber string,\
ProtocolVersion string,\
flow_idle_time float,\
flow_active_time float,\
label boolean,\
subLabel boolean,\
subLabelCat boolean\
) USING iceberg;""").show()


df_renamed.write.mode("append").insertInto(f"nessie.attackdb.attackphase1")





