import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

def cast_df_values(data: DataFrame): DataFrame = {
  data.withColumn("views", data("views").cast(IntegerType))
    .withColumn("likes", data("likes").cast(IntegerType))
    .withColumn("dislikes", data("dislikes").cast(IntegerType))
    .withColumn("comment_count", data("comment_count").cast(IntegerType))
    .withColumn("trending_date", to_date(data("trending_date"), "yy.dd.MM"))
    .withColumn("publish_time", to_timestamp(data("publish_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
		.withColumn("comments_disabled", data("comments_disabled").cast(BooleanType))
		.withColumn("ratings_disabled", data("ratings_disabled").cast(BooleanType))
}

val spark = SparkSession
  .builder()
  .appName("YoutubeVideoAnalyticsSpark")
  .master("local[*]")
  .getOrCreate()
import spark.implicits._

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
val sqlContext = SparkSession.builder().getOrCreate()

val root_dir = "/home/abcoep/Amit_HD/IT/Big_Data/Purdue_Simplilearn_Data_Engineering/YoutubeVideoAnalyticsSpark/"
val resources_dir = root_dir + "src/test/resources/"
val results_dir = root_dir + "results/"
val categoryTitleDf = sqlContext.read.format("avro").option("header", "true").option("inferSchema", "true")
	.load(resources_dir + "category_title*.avro")
println("Category count: " + categoryTitleDf.count())
var usVideosDf = sqlContext.read.format("avro").option("header", "true").option("inferSchema", "true")
  .load(resources_dir + "USvideos*.avro").join(categoryTitleDf, Seq("category_id"), "left")
	.select("video_id", "category_id", "category", "trending_date", "title", "channel_title", "publish_time", "tags", "views", "likes", "dislikes", "comment_count", "comments_disabled", "ratings_disabled")
usVideosDf.printSchema()
println("Video records count: " + usVideosDf.count())

usVideosDf = cast_df_values(usVideosDf)
usVideosDf.printSchema()

val videoIdPartition = Window.partitionBy("video_id")
val overallUsVideosDf = usVideosDf
  .withColumn("latestTrendingDate", max("trending_date").over(videoIdPartition))
  .where(col("trending_date") === col("latestTrendingDate"))
  .drop("latestTrendingDate")

val userInteractionDf = overallUsVideosDf.withColumn("user_interaction", col("views") + col("likes") + col("dislikes") + col("comment_count"))

val top3UserInteraction = userInteractionDf.orderBy(desc("user_interaction")).limit(3).toJSON.collect.mkString("[", "," , "]")

val top3UserInteractionJson = spark.sparkContext.parallelize(s"""{
     |"id": "top3UserInteraction",
     |"name": "Top 3 videos by user interaction",
     |"user_interaction": $top3UserInteraction
  }""".stripMargin :: Nil)
spark.read.json(spark.createDataset(top3UserInteractionJson)).write.json(results_dir + "top_3_user_interaction")

val bottom3UserInteraction = userInteractionDf.orderBy(asc("user_interaction")).limit(3).toJSON.collect.mkString("[", "," , "]")

val bottom3UserInteractionJson = spark.sparkContext.parallelize(s"""{
     |"id": "bottom3UserInteraction",
     |"name": "Bottom 3 videos by user interaction",
     |"user_interaction": $bottom3UserInteraction
  }""".stripMargin :: Nil)
spark.read.json(spark.createDataset(bottom3UserInteractionJson)).write.json(results_dir + "bottom_3_user_interaction")

val videoIdYearPartition = Window.partitionBy("video_id", "year")
val yearlyUsVideosDf = usVideosDf.withColumn("year", year(col("trending_date")))
  .withColumn("latestTrendingDate", max("trending_date").over(videoIdYearPartition))
  .where(col("trending_date") === col("latestTrendingDate"))
  .drop("latestTrendingDate")
val yearlyUserInteractionDf = yearlyUsVideosDf.withColumn("user_interaction", col("views") + col("likes") + col("dislikes") + col("comment_count"))

val categoryPartition = Window.partitionBy("category_id")
val top3ViewsByCategoryYearly = yearlyUserInteractionDf.withColumn("rank", rank().over(categoryPartition.orderBy(col("views").desc)))
  .filter(col("rank") <= 3)
  .drop("rank")
  .toJSON.collect.mkString("[", "," , "]")
val top3CommentCountByCategoryYearly = yearlyUserInteractionDf.filter(!col("comments_disabled"))
	.withColumn("rank", rank().over(categoryPartition.orderBy(col("comment_count").desc)))
  .filter(col("rank") <= 3)
  .drop("rank")
  .toJSON.collect.mkString("[", "," , "]")
val top3LikesByCategoryYearly = yearlyUserInteractionDf.filter(!col("ratings_disabled"))
	.withColumn("rank", rank().over(categoryPartition.orderBy(col("likes").desc)))
  .filter(col("rank") <= 3)
  .drop("rank")
  .toJSON.collect.mkString("[", "," , "]")
val top3UserInteractionByCategoryYearly = yearlyUserInteractionDf.withColumn("rank", rank().over(categoryPartition.orderBy(col("user_interaction").desc)))
  .filter(col("rank") <= 3)
  .drop("rank")
  .toJSON.collect.mkString("[", "," , "]")

val top3ByCategoryYearlyJson = spark.sparkContext.parallelize(
  s"""{
     |"id": "top3ByCategoryYearly",
     |"name": "Top 3 videos of each category in each year",
		 |"views": $top3ViewsByCategoryYearly,
		 |"comment_count": $top3CommentCountByCategoryYearly,
		 |"likes": $top3LikesByCategoryYearly,
		 |"user_interaction": $top3UserInteractionByCategoryYearly
	}""".stripMargin :: Nil)
spark.read.json(spark.createDataset(top3ByCategoryYearlyJson)).write.json(results_dir + "top_3_by_category_yearly")

val videoIdMonthYearPartition = Window.partitionBy("video_id", "year", "month")
val monthlyUsVideosDf = yearlyUsVideosDf.withColumn("month", month(col("trending_date")))
	.withColumn("latestTrendingDate", max("trending_date").over(videoIdMonthYearPartition))
	.where(col("trending_date") === col("latestTrendingDate"))
	.drop("latestTrendingDate")

val monthlyLikeDislikeRatioDf = monthlyUsVideosDf.filter(!col("ratings_disabled"))
	.withColumn("like_dislike_ratio", when(col("likes") + col("dislikes") > 0, (col("likes") - col("dislikes")).cast("float").divide(col("likes") + col("dislikes"))).otherwise(0))
val monthYearPartition = Window.partitionBy("year", "month")
val top3LikesRatioMonthly = monthlyLikeDislikeRatioDf.withColumn("rank", rank().over(monthYearPartition.orderBy(col("like_dislike_ratio").desc, col("likes").desc)))
	.filter(col("rank") <= 3)
	.drop("rank")
	.toJSON.collect.mkString("[", "," , "]")
val top3DislikesRatioMonthly = monthlyLikeDislikeRatioDf.withColumn("rank", rank().over(monthYearPartition.orderBy(col("like_dislike_ratio").asc, col("dislikes").desc)))
	.filter(col("rank") <= 3)
	.drop("rank")
	.toJSON.collect.mkString("[", "," , "]")

val top3LikesDislikesRatioMonthlyJson = spark.sparkContext.parallelize(
	s"""{
		 |"id": "top3LikesDislikesRatioMonthly",
		 |"name": "Top 3 videos of each month by their likes and dislikes ratio",
		 |"likes_ratio": $top3LikesRatioMonthly,
		 |"dislikes_ratio": $top3DislikesRatioMonthly
	}""".stripMargin :: Nil)
spark.read.json(spark.createDataset(top3LikesDislikesRatioMonthlyJson)).write.json(results_dir + "top_3_likes_dislikes_ratio_monthly")

val top3ViewsByCategoryMonthly = monthlyUsVideosDf.withColumn("rank", rank().over(categoryPartition.orderBy(col("views").desc)))
	.filter(col("rank") <= 3)
	.drop("rank")
	.toJSON.collect.mkString("[", "," , "]")
val top3LikesByCategoryMonthly = monthlyUsVideosDf.filter(!col("ratings_disabled"))
	.withColumn("rank", rank().over(categoryPartition.orderBy(col("likes").desc)))
	.filter(col("rank") <= 3)
	.drop("rank")
	.toJSON.collect.mkString("[", "," , "]")
val top3DislikesByCategoryMonthly = monthlyUsVideosDf.filter(!col("ratings_disabled"))
	.withColumn("rank", rank().over(categoryPartition.orderBy(col("dislikes").desc)))
	.filter(col("rank") <= 3)
	.drop("rank")
	.toJSON.collect.mkString("[", "," , "]")

val top3ByCategoryMonthlyJson = spark.sparkContext.parallelize(
	s"""{
		 |"id": "top3ByCategoryMonthly",
		 |"name": "Top 3 videos of each category in each month",
		 |"views": $top3ViewsByCategoryMonthly,
		 |"likes": $top3LikesByCategoryMonthly,
		 |"dislikes": $top3DislikesByCategoryMonthly
	}""".stripMargin :: Nil)
spark.read.json(spark.createDataset(top3ByCategoryMonthlyJson)).write.json(results_dir + "top_3_by_category_monthly")

val channelsDf = overallUsVideosDf.groupBy("channel_title")
	.agg(count("*").alias("total_videos"), sum("views").alias("total_views"), sum("likes").alias("total_likes"), sum("dislikes").alias("total_dislikes"), sum("comment_count").alias("total_comment_count"))
	.withColumn("like_dislike_ratio", when(col("total_likes") + col("total_dislikes") > 0, (col("total_likes") - col("total_dislikes")).cast("float").divide(col("total_likes") + col("total_dislikes"))).otherwise(0))
val top3ChannelsByViews = channelsDf.orderBy(desc("total_views"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")
val top3ChannelsByLikesRatio = channelsDf.orderBy(desc("like_dislike_ratio"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")
val top3ChannelsByDislikesRatio = channelsDf.orderBy(asc("like_dislike_ratio"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")
val top3ChannelsByCommentCount = channelsDf.orderBy(desc("total_comment_count"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")

val top3ChannelsJson = spark.sparkContext.parallelize(
	s"""{
		 |"id": "top3Channels",
		 |"name": "Top 3 channels",
		 |"total_views": $top3ChannelsByViews,
		 |"likes_ratio": $top3ChannelsByLikesRatio,
		 |"dislikes_ratio": $top3ChannelsByDislikesRatio,
		 |"total_comment_count": $top3ChannelsByCommentCount
	}""".stripMargin :: Nil)
spark.read.json(spark.createDataset(top3ChannelsJson)).write.json(results_dir + "top_3_channels")

val categoriesDf = overallUsVideosDf.groupBy("category_id")
	.agg(count("*").alias("total_videos"), sum("views").alias("total_views"), sum("likes").alias("total_likes"), sum("dislikes").alias("total_dislikes"), sum("comment_count").alias("total_comment_count"))
	.withColumn("like_dislike_ratio", when(col("total_likes") + col("total_dislikes") > 0, (col("total_likes") - col("total_dislikes")).cast("float").divide(col("total_likes") + col("total_dislikes"))).otherwise(0))
	.join(categoryTitleDf, Seq("category_id"), "left")
	.select("category_id", "category", "total_videos", "total_views", "total_likes", "total_dislikes", "total_comment_count", "like_dislike_ratio")

val top3CategoriesByViews = categoriesDf.orderBy(desc("total_views"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")
val top3CategoriesByLikesRatio = categoriesDf.orderBy(desc("like_dislike_ratio"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")
val top3CategoriesByDislikesRatio = categoriesDf.orderBy(asc("like_dislike_ratio"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")
val top3CategoriesByCommentCount = categoriesDf.orderBy(desc("total_comment_count"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")

val top3CategoriesJson = spark.sparkContext.parallelize(
	s"""{
		 |"id": "top3Categories",
		 |"name": "Top 3 categories",
		 |"total_views": $top3CategoriesByViews,
		 |"likes_ratio": $top3CategoriesByLikesRatio,
		 |"dislikes_ratio": $top3CategoriesByDislikesRatio,
		 |"total_comment_count": $top3CategoriesByCommentCount
	}""".stripMargin :: Nil)
spark.read.json(spark.createDataset(top3CategoriesJson)).write.json(results_dir + "top_3_categories")

val top3VideosByAvgDailyViews = overallUsVideosDf.withColumn("avg_daily_views", col("views").divide(datediff(col("trending_date"), col("publish_time"))))
	.orderBy(desc("avg_daily_views"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")
val top3VideosByCommentViewRatio = overallUsVideosDf.filter(!col("comments_disabled"))
	.withColumn("comment_view_ratio", col("comment_count").cast("float").divide(col("views")))
	.filter(col("comment_view_ratio") >= 0.005)
	.orderBy(desc("comment_view_ratio"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")
val top3VideosByLikeViewRatio = overallUsVideosDf.filter(!col("ratings_disabled"))
	.withColumn("like_view_ratio", col("likes").cast("float").divide(col("views")))
	.filter(col("like_view_ratio") >= 0.04)
	.orderBy(desc("like_view_ratio"))
	.limit(3)
	.toJSON.collect.mkString("[", "," , "]")

val top3VideosJson = spark.sparkContext.parallelize(
	s"""{
		 |"id": "top3Videos",
		 |"name": "Top 3 videos",
		 |"avg_daily_views": $top3VideosByAvgDailyViews,
		 |"comment_view_ratio": $top3VideosByCommentViewRatio,
		 |"like_view_ratio": $top3VideosByLikeViewRatio
	}""".stripMargin :: Nil)
spark.read.json(spark.createDataset(top3VideosJson)).write.json(results_dir + "top_3_videos")
