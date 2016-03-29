package spark.i.main.numeros;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object NumerosPares extends App {

  val sparkConf = new SparkConf().setAppName("NumberAnalysis").setMaster("local")
  val sc = new SparkContext(sparkConf)

  val listaNumeros = (1 to 10000).toList

  val listaNumerosRdd = sc.parallelize(listaNumeros)
    
  val listaNumerosParesRdd = listaNumerosRdd.filter{ _ % 2 == 0}
  
  val count = listaNumerosParesRdd.count
  
  println("Cantidad de números pares encontrados: "+count);
  
}