package spark.i.main.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.hive.HiveContext

object SqlExample extends App {
  
  val sparkConf = new SparkConf().setAppName("SQL").setMaster("local")
  val sc = new SparkContext(sparkConf)

  val hiveContext = new HiveContext(sc)
  
  //leemos el fichero json
  val userDF = hiveContext.read 
    .json("C:\\personas.json")

  //creamos una tabla temporal para consultar los datos 
  userDF.registerTempTable("persona")
  hiveContext.cacheTable("persona")
  
  val personasDF = hiveContext.sql("SELECT * from persona") //devuelve un DataFrame
  val cntDF = hiveContext.sql("SELECT count(*) AS numPersonas FROM persona")

  //métodos del DF para calcular agregados
  val aggregates = personasDF.agg(max("edad"), min("edad"), count("nombre"))
 
  //muestra los resultados por consola
  cntDF.show()
  aggregates.show()
  
} 