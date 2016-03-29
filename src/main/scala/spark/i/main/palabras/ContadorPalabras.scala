package spark.i.main.palabras

import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ContadorPalabras extends App {

  System.setProperty("hadoop.home.dir", "c:\\hadoop\\")

  val sparkConf = new SparkConf().setAppName("ContadorPalabras").setMaster("local")
  val sc = new SparkContext(sparkConf)
  
  /**
   * función que elimina espacios en blanco adicionales, mayúsculas, 
   * signos de puntuación, exclamación, etc.
   */
  def eliminarPuntuacion(line: String): String = {
       val pattern = new Regex("[a-zA-Z0-9]+")
       return (pattern findAllIn line).mkString(" ").toLowerCase();
  }
  
  //probamos la función
  println(eliminarPuntuacion("Esto es una prueba, una       simple prueba!"))
   
  val shakespeare_texto = sc.textFile(getClass.getClassLoader.getResource("shakespeare.txt").getPath)
  
  val shakespeareRDD_sinPuntuacion = shakespeare_texto.map(eliminarPuntuacion);
  
  //para cada línea, obtener las palabras en una lista mediante flatMap
  val shakespeareRDD_palabras = shakespeareRDD_sinPuntuacion.flatMap { line => line.split(" ")}
  
  //para quitar líneas completas en blanco que aparecen en el texto
  val shakespeareRDD_palabrasSinBlancos = shakespeareRDD_palabras.filter(p => !p.isEmpty()); 
  
  println(shakespeareRDD_palabrasSinBlancos.count())
  
  val shakespeareRDD_contadorPalabras = shakespeareRDD_palabrasSinBlancos
    .map(x => (x,1))
      .reduceByKey((x,y) => x+y)
        .collect() 
  
  /**
   * imprimir las 50 palabras que más aparecen en el texto
   * para ello se ordena de mayor a menor cada par (palabra, apariciones) 
   * mediante shortWith, en base al segundo argumento de cada par, es decir, las apariciones.    
   */
  shakespeareRDD_contadorPalabras.sortWith(_._2 > _._2).take(50).foreach(println) 
   
}