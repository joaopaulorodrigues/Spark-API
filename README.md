# Spark-API
Uma API desenvolvida em python para integração de dados oriundos do Neo4J e do PostegreSQL

------------------------------------ Instruções ------------------------------------------

- Primeiramente instalar:
   - Python 2.7
   - Java 8 (Necessário configurar o $JAVA_HOME)
   - Neo4J 3.4.10
   - PostgreSQL 10.6
   - Apache Spark: spark-2.3.2-bin-hadoop2.7 (Necessário configurar o $SPARK_HOME)
   
- Fazer o download do postgresql-42.2.5.jre6.jar, disponível em https://jdbc.postgresql.org/download/postgresql-42.2.5.jre6.jar
e move-lo para pasta $SPARK_HOME/jars/
  
- Carregar o conjunto de dados cineasts_12k_movies_50k_actors, disponível em https://neo4j.com/developer/example-data/,
no Neo4j

- Carregar o CSV, movies_metadata.csv, disponível em https://www.kaggle.com/rounakbanik/the-movies-dataset#movies_metadata.csv, no PostegreSQL,
mantendo o nome das colunas do arquivo.   

- Inicializar a API ultilizando o comando: $SPARK_HOME/bin/spark-submit api_spark.py --jars

- A API estará rodando em http://127.0.0.1:5000

------------------------------------ Métodos ------------------------------------------

/meanaveragemoviesofactor/<int:actor_id>
  Entrada: id do ator 
  Saída: Retorna a média das avaliações recebidas por filmes em que o ator participou

/meanrevenuemoviesofactor/<int:actor_id>
  Entrada: id do ator 
  Saída: Retorna a média dos faturamentos obtidos por filmes em que o ator participou

/meanaveragemoviesofdirector/<int:director_id>
  Entrada: id do diretor 
  Saída: Retorna a média das avaliações recebidas por filmes que foram dirigidos pelo diretor

/meanrevenuemoviesofdirector/<int:director_id>
  Entrada: id do diretor 
  Saída: Retorna a média dos faturamentos obtidos por filmes que foram dirigidos pelo diretor

/meanprofitmoviesofdirector/<int:director_id>
  Entrada: id do diretor 
  Saída: Retorna a média dos lucros obtidos pelos filmes que foram dirigidos pelo diretor  
  
/popularityActors
  Entrada: não se aplica
  Saída:  Retorna o id, o nome e a soma da popularidade dos 10 atores mais populares, ou seja, os que obtiveram a maior soma de popularidade em filmes que participaram    

/bestprofitadpartnership
  Entrada: não se aplica
  Saída: Retorna o id do ator, o nome do ator, id do diretor, o nome do diretor e a soma dos lucro, da melhor parceria entre ator e diretor, ou seja a que obteve a maior soma de todos lucros dos filmes em que os dois participaram

/bestcriticalaapartnership
  Entrada: não se aplica
  Saída: Retorna o id do primeiro ator, o nome do primeiro ator, id do segundo ator, o nome do segundo ator, a média das avaliações obtidas por filmes em que os dois participaram simultaneamente e quantos filmes aturam juntos, da melhor parceria entre atores, ou seja, dentre as maiores médias de avaliações, as com maiores números de filmes
