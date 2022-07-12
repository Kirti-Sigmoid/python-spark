# function should have more meaningful names
# add more comments for better understanding of the code
# you can separate your business logic with the presentation logic.(i.e separate your main logic in separate functions)


from flask import Flask, app, request, jsonify
from df_formation import new_df
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

app = Flask(__name__)
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

new_df.createOrReplaceTempView("stock_data")


#  maximum moved stock %age wise in both directions (+ve, -ve)
@app.route("/query1", methods=["POST", "GET"])
def query1():
    query = "select Date,Volume,stock_name,Open,Close,High,Low,(((Close-Open)/Close))*100 as max_move from stock_data"
    df1 = spark.sql(query)
    df1.createOrReplaceTempView("temp")
    query_1 = "select Date, stock_name,max_move from df1 order by Volume"
    print(spark.sql(query).show())
    positive_movement = "select t1.Date,t1.stock_name,t1.max_move from temp t1 " \
                        "join ( select Date, Max(max_move) AS positive_move from " \
                        "temp Group By Date) t2 on t1.Date = t2.Date and t2.positive_move=t1.max_move"
    df2 = spark.sql(positive_movement)
    df2.createOrReplaceTempView("t1")
    negative_movement = "select t1.Date,t1.stock_name,t1.max_move from temp t1 join ( select Date, Min(max_move) AS " \
                        "negative_move from temp Group By Date) t2 on t1.Date = t2.Date and " \
                        "t2.negative_move=t1.max_move "
    df3 = spark.sql(negative_movement)
    df3.createOrReplaceTempView("t2")
    df4 = spark.sql(
        "select t1.Date,t1.stock_name as max_stock_name,t1.max_move as maxPercMove, t2.stock_name  as min_stock_name,"
        "t2.max_move as minPercMove from t1 join t2 on t1.Date=t2.Date order by t1.Date")
    return jsonify(json.loads(df4.toPandas().to_json(orient="table", index=False)))


# most traded stock on each day
@app.route("/query2", methods=["POST", "GET"])
def query2():
    # query = "Select Date, MAX(Volume) from query1 where Volume is not null AND Date is not null group by Date" pdf
    # = spark.sql(query) pdf.createOrReplaceTempView("new_table") query = "Select * from query1 where Volume = (
    # Select Volume from new_table) and Date = (Select Date from new_table)"
    query = "Select Date,stock_name from stock_data where Volume is not null AND Date is not null and" \
            "(Date, Volume) IN (Select Date, MAX(Volume) " \
            "from stock_data  " \
            "group by Date) order by Date"
    result = spark.sql(query)
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


# max swing of stock opening from the previous day close price
@app.route("/query3", methods=["POST", "GET"])
def query3():
    result = spark.sql("with added_previous_close as (select stock_name, Date, Open, Close, LAG(Close,1,0) over ( "
                       "partition by stock_name order by Date) as previous_close from stock_data ASC) select "
                       "stock_name, "
                       "ABS(Open - previous_close) as max_swing from added_previous_close order by max_swing DESC "
                       "limit 1 ")

    return json.loads(result.toPandas().to_json(orient="table", index=False))


# stock has moved maximum from 1st Day data to the highest day data
@app.route("/query4", methods=["POST", "GET"])
def query4():
    query = "select distinct stock_name, abs((first_value(Open) over (partition by stock_name order by Date asc) - " \
            "first_value(Close) over (partition by stock_name order by Date desc))) as diff " \
            "from stock_data "
    answer = spark.sql(query)
    # print(answer.show())
    answer.createOrReplaceTempView("max_stock_table")
    query2 = "select stock_name, diff from max_stock_table order by diff desc limit(1)"
    result = spark.sql(query2)
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


# standard deviations for each stock over the period
@app.route("/query5", methods=["POST", "GET"])
def query5():
    query = "Select stock_name, std(Volume) from stock_data group by stock_name"
    result = spark.sql(query)
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


# mean and median prices for each stock
@app.route("/query6", methods=["POST", "GET"])
def query6():
    query = "Select stock_name, avg(Open) as mean_open, percentile_approx(Open,0.5) as median_open " \
            " from stock_data group by stock_name"

    result = spark.sql(query)
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


# average volume over the period
@app.route("/query7", methods=["POST", "GET"])
def query7():
    query = "select stock_name, AVG(Volume) as Average_of_stock from stock_data group by stock_name"
    result = spark.sql(query)
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


# stock has higher average volume
@app.route("/query8", methods=["POST", "GET"])
def query8():
    query = "Select stock_name, AVG(Volume) as avg_vol from stock_data " \
            "group by stock_name order by avg_vol desc limit 1"
    result = spark.sql(query)
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


# highest and lowest prices for a stock over the period of time
@app.route("/query9", methods=["POST", "GET"])
def query9():
    query = "Select stock_name,max(High) as Highest, min(Low) as Lowest from " \
            "stock_data group by stock_name"

    result = spark.sql(query)
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


if __name__ == '__main__':
    app.run(debug=True, port=2000)
