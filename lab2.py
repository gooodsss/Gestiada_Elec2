from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab2").getOrCreate()

df = spark.read.csv("netflix_data.csv", header=True, inferSchema=True)

df.printSchema()

# Partition Strategy 1

partition_country = df.repartition("country")

country_summary = partition_country.filter(df.country == "United States").groupBy("type").count().orderBy("count", ascending=False)

print("Summary of US Content:")
country_summary.show()

# Partition Strategy 2

partition_year = df.repartition(5, "release_year")

year_summary = partition_year.filter(df.release_year >= 2010).groupBy("release_year").count().orderBy("release_year")

print("Content by Release Year:")
year_summary.show()

# Transformations

#Filter country
philippines_data = df.filter(df.country.contains("Philippines"))

print("Phillipine Content:")
philippines_data.select("title", "release_year", "country").show()

# Sort by year

sorted_movies = df.orderBy("release_year", ascending=False)

print("Latest Movies:")
sorted_movies.select("title", "release_year").show(10)

spark.stop()