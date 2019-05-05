# Objectives: Spark data prep
# 1. Read 36 csv files into a Spark dataframe
# 2. Filter the data from UTM-10 (longitude -126 to -120, West Coast North of Santa Barbara)
#      to Puget Sound (lon -123.75 to -122, lat 47.0 to 49.45, Olympia, WA to Vancouver, BC)
# 3. Split the table into "vessel motion" columns, and "vessel attribute" metadata columns.
# 4. Sort the filtered dataframe by time and save as "puget_vessels_3y_pqt".
#       It preserves the partitions created by Spark, and does not repartition into year-month-day yet
# 5. Group-By "mmsi", the Device ID to get Vessel Tracks for each Ship.
# 6. Create a 3-month sample for exploring vessel tracks on a PC
# -------------------------------------------------------
# VM Setup:
# 1. Software: Any Cloud Ubuntu VM (I used Azure) with 
#      1.1 Java 8 installed
#      1.2 Spark 2.4.0 downloaded & extracted
#      1.3 R installed. I used R 3.5.2
# 2. Add a 300 GB datadisk (This is an Azure term. It's the same as an EBS Volume on AWS)
#      2.1 Partition, Format as ext4, and Mount it at /home/ubuntu/datadisk
#      2.2 Make sure the datadisk is writeable. It will be used to persist a Spark DataFrame. 
#            The VM has only 30 GB storage. See the "spark.local.dir" environment variable
# 3. Use an instance with at least 64 GB RAM and 8 CPUs
# -------------------------------------------------------
# Data Setup:
# 1. https://marinecadastre.gov/ais/ . Use the filter "Zone10" in the box
# 2. Download the 36 zip files (2105-2017, monthly) for UTM Zone 10 (Longitude -126 to -120) 
#      Use wget <link>. Do not Click the link. Clicking will download to your desktop, not to the VM
# 3. $ wget https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2017/AIS_2017_01_Zone10.zip
# 4. $ unzip '*.zip'
# 5. $ mv *.csv /home/ubuntu/datadisk/input/
# -------------------------------------------------------

# Set the environment variable "JAVA_HOME" in R
Sys.setenv(JAVA_HOME = "/usr/lib/jvm/java-8-openjdk-amd64")
Sys.setenv(SPARK_HOME = "/home/ubuntu/spark-2.4.0")

# Import all packages BEFORE SparkR, to avoid object masking
library(magrittr)

# Import SparkR
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

conf <- list(spark.driver.memory = "56g",spark.driver.maxResultSize = "48g", spark.local.dir = "/home/ubuntu/datadisk")
sparkR.session(master = "local[*]", sparkConfig = conf)
setwd('/home/ubuntu/datadisk/output')

# Define the Schema
schema <- structType(structField("mmsi", "integer"),
                     structField("base_dt", "timestamp"),
                     structField("lat", "double"),
                     structField("lon", "double"),
                     structField("sog", "float"),
                     structField("cog", "float"),
                     structField("heading", "float"),
                     structField("vessel_name", "string"),
                     structField("imo", "string"),
                     structField("call_sign", "string"),
                     structField("vessel_type", "integer"),
                     structField("status", "string"),
                     structField("l", "float"),
                     structField("w", "float"),
                     structField("draft", "float"),
                     structField("cargo", "integer"))

# Read the CSV files into a Spark dataframe
files <- "/home/ubuntu/datadisk/input/AIS*"
df <- loadDF(path=files, source="csv", schema=schema, header = "true", inferSchema = "false")

# Filter to Puget Sound: Longitude to [-123.75, -122] & Latitude to [47, 49.45]
dfByTime <- df %>% select('mmsi', 'status', 'base_dt', 'lat', 'lon', 'sog', 'cog', 'heading') %>%
filter("(lat BETWEEN 47 AND 49.45) AND (lon BETWEEN -123.75 AND -122)") %>%
arrange(asc(df$base_dt))

# Persist this DataFrame so we don't have to repeat this computation (filtering & sorting by time)
persist(dfByTime, "MEMORY_AND_DISK")

# Save the time-sorted "motion" table to parquet
parquet_file_1 <- '/home/ubuntu/datadisk/output/puget_vessels_3y_pqt/'
write.parquet(dfByTime, parquet_file_1, mode='ignore')

# Group-By "mmsi" to get Vessel Tracks (3-months & 3-years) for each vessel
tracks_gdf <- dfByTime %>% groupBy('mmsi') %>%
summarize(base_dts=collect_list(dfByTime$base_dt),
          lats=collect_list(dfByTime$lat),
          lons=collect_list(dfByTime$lon),
          sogs=collect_list(dfByTime$sog),
          cogs=collect_list(dfByTime$cog),
          headings=collect_list(dfByTime$heading),
          statuses=collect_list(dfByTime$status))

parquet_file_2 <- '/home/ubuntu/datadisk/output/tracks_3y_pqt/'
write.parquet(tracks_gdf, parquet_file_2, mode='ignore')

# Vessel Metadata
# This data is sent every 3-minutes, only when the vessel is not at anchor
# It does not change.
# Create a custom aggregation function, "fnna" to get the first non-NA (non-missing) value
fnna <- function(vec) {
    a <- first(na.omit(vec))
    return(a)
}

meta_gdf <- df %>% 
select('mmsi', 'vessel_type', 'vessel_name', 'imo', 'call_sign', 'l', 'w', 'draft', 'cargo') %>% 
groupBy('mmsi') %>% 
summarize(vessel_type=fnna(df$vessel_type),
          vessel_name=fnna(df$vessel_name),
          imo=fnna(df$imo),
          call_sign=fnna(df$call_sign),
          cargo=fnna(df$cargo),
          l=mean(df$l),
          w=mean(df$w),
          draft=mean(df$draft))

meta2 <- arrange(meta_gdf, asc(meta_gdf$vessel_type), asc(meta_gdf$mmsi))
meta_coll <- collect(meta2)  # This is an R DataFrame (not a Spark DataFrame)

# Save the Vessel Metadata as a CSV file.
write_file <- '/home/ubuntu/datadisk/output/metadata_3y.csv'
write.csv(meta_coll, write_file, quote=FALSE, na="", row.names=FALSE)

sparkR.session.stop()