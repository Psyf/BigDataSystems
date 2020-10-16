import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{Word2Vec, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    // TODO: what can I cache?
    // TODO: How to profile spark programs?

    val stopwords = spark.read.textFile(args(1)).collect() //broadcast this?

    val df = spark.read.csv(args(0))
                       .select("_c5")

    val tokenizer = new Tokenizer().setInputCol("_c5").setOutputCol("tokens")
    val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("tweets").setStopWords(stopwords)
    val clean_df = remover.transform(tokenizer.transform(df)).select("tweets")

    clean_df.cache()

    val w2v_model = new Word2Vec().setInputCol("tweets").setOutputCol("vector").fit(clean_df)
    val vectors = w2v_model.transform(clean_df).select("vector")

    clean_df.unpersist()
    //vectors.cache() //uncomment in cluster

    // TODO : Try a few more k values
    // TODO : OOM, set memory higher
    val km_model = new KMeans().setK(10).setMaxIter(50).setFeaturesCol("vector").fit(vectors)
    km_model.clusterCenters.foreach(println)

    // https://medium.com/swlh/k-means-clustering-using-pyspark-on-data-bricks-fd65e207154a
    val predictions = km_model.transform(vectors)
    val evaluator = new ClusteringEvaluator()
    println("Silhouette with squared euclidean distance = %d", evaluator.evaluate(predictions))

    // TODO:
    //  get most freq words of each cluster
    //  get silhouette value of clustering. result


    spark.stop()
  }
}


