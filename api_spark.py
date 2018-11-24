import json
import flask
from flask import request, jsonify
from pyspark import SparkContext
from pyspark import sql
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import client
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
import sys
reload(sys)
sys.setdefaultencoding('utf8')


app = flask.Flask(__name__)
app.config["DEBUG"] = True

db = GraphDatabase("http://localhost:7474", username="neo4j", password = "admin")

sc = SparkContext(appName = "movies")
sqlContext = sql.SQLContext(sc)

url = 'postgresql://localhost:5432/postgres'

properties = {'user': 'postgres', 'password': 'postgres'}
dfpostgres = DataFrameReader(sqlContext).jdbc(
    url='jdbc:%s' % url, table='movie_metadata', properties=properties
)


@app.route('/averagevotemoviesofactor/<string:actor_name>', methods=['GET'])
def average_vote_movies_of_actor(actor_name):
    q = 'MATCH (a:Actor)-[r:ACTS_IN]->(m:Movie) WHERE a.name= "' + actor_name+'" RETURN m.imdbId'
    results = db.query(q, returns=(str))
    rdd = sc.parallelize(results)
    
    schema = StructType([StructField('imdbId', StringType(), True),
                         ])
        
    df = sqlContext.createDataFrame(rdd, schema)
    dfj = df.join(dfpostgres, df.imdbId == dfpostgres.imdb_id)
    dfj = dfj.agg({'vote_average': 'mean'})
    mean = {}
    mean["vote_average"] = dfj.rdd.map(tuple).take(1)[0][0]
    
    return json.dumps(mean, indent=4, separators=(',', ': '))
    #return q
    
@app.route('/averagerevenuemoviesofactor/<string:actor_name>', methods=['GET'])
def average_revenue_movies_of_actor(actor_name):
    q = 'MATCH (a:Actor)-[r:ACTS_IN]->(m:Movie) WHERE a.name= "' + actor_name +'" RETURN m.imdbId'
    results = db.query(q, returns=(str))
    rdd = sc.parallelize(results)
    
    schema = StructType([StructField('imdbId', StringType(), True),
                         ])
        
    df = sqlContext.createDataFrame(rdd, schema)
    dfj = df.join(dfpostgres, df.imdbId == dfpostgres.imdb_id)
    dfj = dfj.agg({'revenue': 'mean'})
    mean = {}
    mean["revenue"] = dfj.rdd.map(tuple).take(1)[0][0]
    
    return json.dumps(mean, indent=4, separators=(',', ': '))

@app.route('/averagevotemoviesofdirector/<string:director_name>', methods=['GET'])
def average_vote_movies_of_director(director_name):
    q = 'MATCH (a:Director)-[r:DIRECTED]->(m:Movie) WHERE a.name= "' + director_name+'" RETURN m.imdbId'
    results = db.query(q, returns=(str))
    rdd = sc.parallelize(results)
    schema = StructType([StructField('imdbId', StringType(), True),
                         ])
        
    df = sqlContext.createDataFrame(rdd, schema)
    dfj = df.join(dfpostgres, df.imdbId == dfpostgres.imdb_id)
    dfj = dfj.agg({'vote_average': 'mean'})
    mean = {}
    mean["vote_average"] = dfj.rdd.map(tuple).take(1)[0][0]
    
    return json.dumps(mean, indent=4, separators=(',', ': '))

@app.route('/averagerevenuemoviesofdirector/<string:director_name>', methods=['GET'])
def average_revenue_movies_of_director(director_name):
    q = 'MATCH (a:Director)-[r:DIRECTED]->(m:Movie) WHERE a.name= "' + director_name +'" RETURN m.imdbId'
    results = db.query(q, returns=(str))
    rdd = sc.parallelize(results)
    
    schema = StructType([StructField('imdbId', StringType(), True),
                         ])
        
    df = sqlContext.createDataFrame(rdd, schema)
    dfj = df.join(dfpostgres, df.imdbId == dfpostgres.imdb_id)
    dfj = dfj.agg({'revenue': 'mean'})
    mean = {}
    mean["revenue"] = dfj.rdd.map(tuple).take(1)[0][0]
    
    return json.dumps(mean, indent=4, separators=(',', ': '))
  
@app.route('/averageprofitmoviesofdirector/<string:director_name>', methods=['GET'])
def average_profit_movies_of_director(director_name):
    q = 'MATCH (a:Director)-[r:DIRECTED]->(m:Movie) WHERE a.name= "' + director_name +'" RETURN m.imdbId'
    results = db.query(q, returns=(str))
    rdd = sc.parallelize(results)
    schema = StructType([StructField('imdbId', StringType(), True),
                         ])
        
    df = sqlContext.createDataFrame(rdd, schema)
    dfj = df.join(dfpostgres, df.imdbId == dfpostgres.imdb_id)
    revenue = dfj.agg({'revenue': 'mean'})
    budget = dfj.agg({'budget': 'mean'})
    mean = {}
    mean["revenue"] = revenue.rdd.map(tuple).take(1)[0][0]
    mean["budget"] = budget.rdd.map(tuple).take(1)[0][0]
    mean["profit"] = mean["revenue"] - mean["budget"]
    
    return json.dumps(mean, indent=4, separators=(',', ': '))

@app.route('/popularityActors', methods=['GET'])
def popularityActors():
    q = 'MATCH (a:Actor)-[r:ACTS_IN]->(m:Movie) RETURN m.imdbId, a.id, a.name'
    results = db.query(q, returns=(str, int, str))
    rdd = sc.parallelize(results)
    
    schema = StructType([StructField('imdbId', StringType(), True),
                         StructField('actor_id', IntegerType(), True),
                         StructField('name', StringType(), True),
                         ])
        
    df = sqlContext.createDataFrame(rdd, schema)
    dfj = df.join(dfpostgres, df.imdbId == dfpostgres.imdb_id)
    popularity = dfj.groupby('actor_id', 'name').agg({'popularity': 'sum'})
    
    popularity = popularity.sort(desc('sum(popularity)')).limit(10)
    
    mean = {}
    top10 = []
    for i in range(0,10):
        mean["actor_id"] = popularity.rdd.map(tuple).take(10)[i][0]
        mean["name"] = popularity.rdd.map(tuple).take(10)[i][1]
        mean["popularity"] = popularity.rdd.map(tuple).take(10)[i][2]
        top10.append(mean)
        mean = {}
        
    return json.dumps(top10, indent=4, separators=(',', ': '))

@app.route('/bestprofitadpartnership', methods=['GET'])
def best_profit_ad_partnership():
    q = 'MATCH (a:Actor)-[r:ACTS_IN]->(m:Movie)<-[:DIRECTED]-(b:Director) RETURN m.imdbId, a.name, a.id, b.id, b.name'
    results = db.query(q, returns=(str, str, int, int, str))
    rdd = sc.parallelize(results)
    schema = StructType([StructField('imdbId', StringType(), True),
                         StructField('name_actor', StringType(), True),
                         StructField('actor_id', IntegerType(), True),
                         StructField('director_id', IntegerType(), True),
                         StructField('name_director', StringType(), True),
                         ])
        
    df = sqlContext.createDataFrame(rdd, schema)
    dfj = df.join(dfpostgres, df.imdbId == dfpostgres.imdb_id)
    best = dfj.groupby('actor_id','name_actor', 'director_id', 'name_director').agg({'revenue': 'sum'})
    best = best.sort(desc('sum(revenue)'))
    mean = {}
    mean["actor_id"] = best.rdd.map(tuple).take(2)[1][0]
    mean["name_actor"] = best.rdd.map(tuple).take(2)[1][1]
    mean["director_id"] = best.rdd.map(tuple).take(2)[1][2]
    mean["name_director"] = best.rdd.map(tuple).take(2)[1][3]
    mean["revenue"] = best.rdd.map(tuple).take(2)[1][4]
    
    
    return json.dumps(mean, indent=4, separators=(',', ': '))
    
@app.route('/bestcriticalaapartnership', methods=['GET'])
def best_critical_aa_partnership():
    q = 'MATCH (a:Actor)-[:ACTS_IN]->(m:Movie)<-[:ACTS_IN]-(b:Actor) RETURN m.imdbId, a.name, a.id, b.id, b.name'
    results = db.query(q, returns=(str, str, int, int, str))
    rdd = sc.parallelize(results)
    schema = StructType([StructField('imdbId', StringType(), True),
                         StructField('name_actor1', StringType(), True),
                         StructField('actor1_id', IntegerType(), True),
                         StructField('actor2_id', IntegerType(), True),
                         StructField('name_actor2', StringType(), True),
                         ])
        
    df = sqlContext.createDataFrame(rdd, schema)
    dfj = df.join(dfpostgres, df.imdbId == dfpostgres.imdb_id)
    best = dfj.filter(col('vote_average') < 10).groupby('actor1_id','name_actor1', 'actor2_id', 'name_actor2').agg({'vote_average': 'mean','imdbId': 'count'})
    best = best.sort(desc('count(imdbId)'), desc('avg(vote_average)'))
    mean = {}
    mean["actor1_id"] = best.rdd.map(tuple).take(2)[1][0]
    mean["name_actor1"] = best.rdd.map(tuple).take(2)[1][1]
    mean["actor2_id"] = best.rdd.map(tuple).take(2)[1][2]
    mean["name_actor2"] = best.rdd.map(tuple).take(2)[1][3]
    mean["vote_average"] = best.rdd.map(tuple).take(2)[1][5]
    mean["count_movies"] = best.rdd.map(tuple).take(2)[1][4]
    
    
    return json.dumps(mean, indent=4, separators=(',', ': '))

app.run()