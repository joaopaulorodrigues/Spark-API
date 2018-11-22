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

#Configura a API
app = flask.Flask(__name__)
app.config["DEBUG"] = True

#conecta o Neo4J
db = GraphDatabase("http://localhost:7474", username="neo4j", password = "admin")

#cria o contexto do Spark e do SparkSQL
sc = SparkContext(appName = "movies")
sqlContext = sql.SQLContext(sc)

#Conecta o PostegreSQL e transforma a tabela movie_metadata em um dataframe
url = 'postgresql://localhost:5432/postgres'

properties = {'user': 'postgres', 'password': 'postgres'}
dfpostgres = DataFrameReader(sqlContext).jdbc(
    url='jdbc:%s' % url, table='movie_metadata', properties=properties
)

#Entrada: id do ator 
#Saída: Retorna a média das avaliações recebidas por filmes em que o ator participou
@app.route('/meanaveragemoviesofactor/<int:actor_id>', methods=['GET'])
def mean_average_movies_of_actor(actor_id):
    q = 'MATCH (a:Actor)-[r:ACTS_IN]->(m:Movie) WHERE a.id= "' + str(actor_id)+'" RETURN m.imdbId'
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

#Entrada: id do ator 
#Saída: Retorna a média dos faturamentos obtidos por filmes em que o ator participou
@app.route('/meanrevenuemoviesofactor/<int:actor_id>', methods=['GET'])
def mean_revenue_movies_of_actor(actor_id):
    q = 'MATCH (a:Actor)-[r:ACTS_IN]->(m:Movie) WHERE a.id= "' + str(actor_id)+'" RETURN m.imdbId'
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

#Entrada: id do diretor 
#Saída: Retorna a média das avaliações recebidas por filmes que foram dirigidos pelo diretor
@app.route('/meanaveragemoviesofdirector/<int:director_id>', methods=['GET'])
def mean_average_movies_of_director(director_id):
    q = 'MATCH (a:Director)-[r:DIRECTED]->(m:Movie) WHERE a.id= "' + str(director_id)+'" RETURN m.imdbId'
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

#Entrada: id do diretor 
#Saída: Retorna a média dos faturamentos obtidos por filmes que foram dirigidos pelo diretor
@app.route('/meanrevenuemoviesofdirector/<int:director_id>', methods=['GET'])
def mean_revenue_movies_of_director(director_id):
    q = 'MATCH (a:Director)-[r:DIRECTED]->(m:Movie) WHERE a.id= "' + str(director_id)+'" RETURN m.imdbId'
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
  
#Entrada: id do diretor 
#Saída: Retorna a média dos lucros obtidos pelos filmes que foram dirigidos pelo diretor    
@app.route('/meanprofitmoviesofdirector/<int:director_id>', methods=['GET'])
def mean_profit_movies_of_director(director_id):
    q = 'MATCH (a:Director)-[r:DIRECTED]->(m:Movie) WHERE a.id= "' + str(director_id)+'" RETURN m.imdbId'
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

#Entrada: não se aplica
#Saída:  Retorna o id, o nome e a soma da popularidade dos 10 atores mais populares, ou seja, os que obtiveram a maior soma de popularidade em filmes que participaram    
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

#Entrada: não se aplica
#Saída: Retorna o id do ator, o nome do ator, id do diretor, o nome do diretor e a soma dos lucro, da melhor parceria entre ator e diretor, ou seja a que obteve a maior soma de todos lucros dos filmes em que os dois participaram
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
    
#Entrada: não se aplica
#Saída: Retorna o id do primeiro ator, o nome do primeiro ator, id do segundo ator, o nome do segundo ator, a média das avaliações obtidas por filmes em que os dois participaram simultaneamente e quantos filmes aturam juntos, da melhor parceria entre atores, ou seja, dentre as maiores médias de avaliações, as com maiores números de filmes
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

#inicializa a API
app.run()
