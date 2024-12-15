# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,row_number,when
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, IntegerType, StringType,DateType,BooleanType,DecimalType
from pyspark.sql.functions import year,month,dayofmonth,lower,regexp_replace

# COMMAND ----------

spark=SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

# COMMAND ----------

ball_by_ball_Schema=StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])
ball_by_ball_df=spark.read.option("header","true").format("csv").schema(ball_by_ball_Schema).load("dbfs:/FileStore/tables/Ball_By_Ball.csv")

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

match_df=spark.read.format("csv").option("header","true").schema(match_schema).load("dbfs:/FileStore/tables/Match.csv")

# COMMAND ----------


player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

player_df=spark.read.format("csv").option("header","true").schema(player_schema).load("dbfs:/FileStore/tables/Player.csv")

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

player_match_df=spark.read.format("csv").option("header","true").schema(player_match_schema).load("dbfs:/FileStore/tables/Player_match.csv")

# COMMAND ----------


team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])
team_df=spark.read.format("csv").option("header","true").schema(team_schema).load("dbfs:/FileStore/tables/Team.csv")

# COMMAND ----------

#filter to inclue only valid balls no wide ball or no balls

ball_by_ball_valid_df=ball_by_ball_df.filter((col("wides")==0) & (col("noballs")==0))
#Total runs in each match each innings

ball_by_ball_total_runs_each_inn=ball_by_ball_df.groupBy("match_id","innings_no").agg(
    sum("runs_scored")
)
ball_by_ball_total_runs_each_inn.show()

# COMMAND ----------

#total runs each over 
windowspec=Window.partitionBy("match_id","innings_no").orderBy("over_id")
ball_by_ball_total_runs_each_over=ball_by_ball_df.withColumn(
    "total_runs_each_over",
    sum("runs_scored").over(windowspec)
)
ball_by_ball_total_runs_each_over.select("match_id","innings_no","over_id","total_runs_each_over").show()

# COMMAND ----------

# high impact balls
ball_by_ball_high_impact_ball=ball_by_ball_df.withColumn(
    "high_impact_ball",
    when((col("runs_scored")+col("extra_runs")>6) | (col("bowler_wicket")==1),1).otherwise(0)
)
ball_by_ball_high_impact_ball.select("match_id","runs_scored","innings_no","high_impact_ball").where(col("high_impact_ball")==1).show()

# COMMAND ----------

#analysis from match data
# extracting year,month and day
year_of_the_match=match_df.withColumn("year",year("match_date"))
month_of_the_match=match_df.withColumn("month",month("match_date"))
day_of_the_match=match_df.withColumn("day",dayofmonth("match_date"))

# match win by high margin
win_match_by_high_medium_low_margin=match_df.withColumn(
    "high_margin",when(col("win_margin")>=100,"high")
    .when((col("win_margin")<100) & (col("win_margin")>=50),"medium")
    .otherwise("low")
)
win_match_by_high_medium_low_margin.show(5)

# COMMAND ----------


# player data analysis

player_df=player_df.withColumn("player_name",lower(regexp_replace("player_name","[^a-zA-Z0-9]","")))

#handling missing values

player_df=player_df.na.fill({"batting_hand":"unknown","bowling_skill":"unknown"})


# COMMAND ----------

from pyspark.sql.functions import expr,current_date,when
#is player is veteran
player_match_df=player_match_df.withColumn("Veteran_or_non_veteran",
    when(col("age_as_on_match")>=35,"vereran").otherwise("Non-veteran")
    )

player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)


# COMMAND ----------

#Creating temp views for all dfs

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")

# COMMAND ----------

# top scoring battsam per season
top_scoring_battsman_per_season=spark.sql("""
    select p.player_name,m.season_year,sum(b.runs_scored) as total_runs from ball_by_ball b 
    JOIN match m ON b.match_id=m.match_id
    JOIN player_match pm ON m.match_id=pm.match_id
    JOIN player p ON p.player_id=pm.player_id
    group by p.player_name,m.season_year
    order by m.season_year,total_runs desc
                                          """)

top_scoring_battsman_per_season.show(5)

# COMMAND ----------

# toss impactof individual matches

toss_impact_of_individual_match=spark.sql("""
        select match_id,toss_winner,toss_name,match_winner,
            case 
                when toss_winner=match_winner then "won" else "loss"
            end as outcome
        from match
        order by match_id
                                         """)
toss_impact_of_individual_match.show(5)                                         

# COMMAND ----------

average_runs_in_wins = spark.sql("""
SELECT p.player_name, AVG(b.runs_scored) AS avg_runs_in_wins, COUNT(*) AS innings_played
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON pm.player_id = p.player_id
JOIN match m ON pm.match_id = m.match_id
WHERE m.match_winner = pm.player_team
GROUP BY p.player_name
ORDER BY avg_runs_in_wins DESC
""")
average_runs_in_wins.show(5)

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

average_runs_pd = average_runs_in_wins.toPandas()

# Using seaborn to plot average runs in winning matches
plt.figure(figsize=(12, 8))
top_scorers = average_runs_pd.nlargest(10, 'avg_runs_in_wins')
sns.barplot(x='player_name', y='avg_runs_in_wins', data=top_scorers)
plt.title('Average Runs Scored by Batsmen in Winning Matches (Top 10 Scorers)')
plt.xlabel('Player Name')
plt.ylabel('Average Runs in Wins')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

scores_by_venue = spark.sql("""
SELECT venue_name, AVG(total_runs) AS average_score, MAX(total_runs) AS highest_score
FROM (
    SELECT ball_by_ball.match_id, match.venue_name, SUM(runs_scored) AS total_runs
    FROM ball_by_ball
    JOIN match ON ball_by_ball.match_id = match.match_id
    GROUP BY ball_by_ball.match_id, match.venue_name
)
GROUP BY venue_name
ORDER BY average_score DESC
""")

# COMMAND ----------

scores_by_venue_pd = scores_by_venue.toPandas()

# Plot
plt.figure(figsize=(14, 8))
sns.barplot(x='average_score', y='venue_name', data=scores_by_venue_pd)
plt.title('Distribution of Scores by Venue')
plt.xlabel('Average Score')
plt.ylabel('Venue')
plt.show()

# COMMAND ----------


