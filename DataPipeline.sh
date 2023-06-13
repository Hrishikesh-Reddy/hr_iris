
hive -e "
drop database if exists hr_iris_landing cascade;
create database hr_iris_landing;
use hr_iris_landing;
CREATE EXTERNAL TABLE IF NOT EXISTS iris
(
  Id INT,
  SepalLengthCm DOUBLE,
  SepalWidthCm DOUBLE,
  PetalLengthCm DOUBLE,
  PetalWidthCm DOUBLE,
  Species STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/bigdatacloudxlab27228/hdfs_iris_landing/'
TBLPROPERTIES ('skip.header.line.count'='1');
"


pyspark_commands=$(cat <<'END_PYSPARK_COMMANDS'
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName('hrishi') \
    .enableHiveSupport() \
    .getOrCreate()
df = spark.sql('SELECT * FROM hr_iris_landing.iris')
df = df.filter(df.id.isNotNull())
df = df.fillna('N/A')
df.write.mode('overwrite').parquet('/user/bigdatacloudxlab27228/hdfs_iris_curated')
df.write.mode('overwrite').csv('/user/bigdatacloudxlab27228/hdfs_iris_curated.csv')
spark.stop()
END_PYSPARK_COMMANDS
)
echo "$pyspark_commands" | pyspark


mysql_commands=$(cat <<'END_MYSQL_COMMANDS'
use sqoopex;
DROP TABLE IF EXISTS iris;
CREATE TABLE IF NOT EXISTS iris (
  Id INT,
  SepalLengthCm DOUBLE,
  SepalWidthCm DOUBLE,
  PetalLengthCm DOUBLE,
  PetalWidthCm DOUBLE,
  Species VARCHAR(255)
);
END_MYSQL_COMMANDS
)

echo "$mysql_commands" | mysql -h cxln2.c.thelab-240901.internal -u sqoopuser -p'NHkkP876rp'

sqoop export --connect jdbc:mysql://cxln2:3306/sqoopex --username sqoopuser --password NHkkP876rp --table iris --export-dir hdfs://cxln1.c.thelab-240901.internal:8020/user/bigdatacloudxlab27228/hdfs_iris_curated.csv --input-fields-terminated-by ',' --input-lines-terminated-by '\n'
# End of the script