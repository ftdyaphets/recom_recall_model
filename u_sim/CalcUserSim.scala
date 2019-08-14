package u_sim

import utils.BaseUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import scopt.OptionParser
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, MinHashLSH}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col



object CalcUserSim {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("calc_u_sim")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    //remove_existed_preprocess_dirs(params)

    val data_rating = get_data_rating(sc, params.data_rating_input_path)
    BaseUtils.print_rdd(data_rating.map(_.toString()), "data_rating")

    println("calc u_sim by jaccard distance with minHashLsh ...")
    val rating_sim = get_dataRating_minHashLsh(spark, data_rating, 100, 10)
    BaseUtils.print_rdd(rating_sim.map(_.toString()), "rating_sim")

    println("calc u_sim by euclidean distance with bucketedRandom Lsh ...")
    val user_features = get_user_feature(sc, params)
    BaseUtils.print_rdd(user_features.sortBy(_._1).map(_.toString), "user_features")
    val fea_sim = get_feature_minHashLsh(spark, user_features)
    BaseUtils.print_rdd(fea_sim.map(_.toString()), "fea_sim")

    val sim_results = fea_sim.union(rating_sim).map(x => ((x._1, x._2), x._3))
      .groupByKey().mapValues {
      x =>
        val size = x.size

    }


  }




  def get_data_rating(sc: SparkContext, data_rating_input_path: String) = {
    val data_rating = sc.textFile(data_rating_input_path).map {
      line =>
        val str_arr = line.split("\\|", -1)
        val user_id = str_arr(0)
        val item_id = str_arr(1)
        val rating = str_arr(2).toDouble
        ((user_id, item_id), rating)
    }.reduceByKey(_ + _).map (x => (x._1._1, x._1._2, x._2))
    data_rating
  }



  def normalize_rating(data_rating: RDD[(String, String, Double)], low: Int, high: Int) = {
    val rating_list = data_rating.map(_._3)
    val max = rating_list.max()
    val min = rating_list.min()
    val k = (high - low) / (max - min)
    val normalized_data_rating = data_rating.map {
      rating =>
        val normalized_rating = low + k * (rating._3 - min)
        (rating._1, rating._2, normalized_rating)
    }
    normalized_data_rating
  }


  def get_indexed_feature(features: RDD[(String, String)]) = {
    val indexed_features = features.flatMapValues {
      feature =>
        val feature_list = feature.split("\\|", -1).zipWithIndex.map {
          case (fea, index) => (index + 1, fea)
        }
        feature_list
    }.filter(_._1.toInt <= 30000).map(x => (x._1, x._2._1.toString, x._2._2.toDouble))
    indexed_features
  }

  def get_user_feature(sc: SparkContext, params: Params) = {
    val u_each_item_count_sta = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_each_item_count_sta"))
    //BaseUtils.print_rdd(u_each_item_count_sta.map(_.toString()), "u_each_item_count_sta")
    val u_total_sessions_rank_percent  = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_total_sessions_rank_percent"))
    //BaseUtils.print_rdd(u_total_sessions_rank_percent.map(_.toString()),
     // "u_total_sessions_rank_percent")
    //val u_meta_info  = split_uid_and_features(sc.textFile(
    // params.user_features_input_path + "u_meta_info"))
    //val u_items_top5 = split_uid_and_features(sc.textFile(
    // params.user_features_input_path + "u_items_top5"))
    val u_item_price_sta = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_item_price_sta"))
    val u_total_money_rank_percent = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_total_money_rank_percent"))
    val u_basket_size_sta = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_basket_size_sta"))
    val u_basket_money_slot_percent  = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_basket_money_slot_percent"))
    val u_order_days_ratio = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_order_days_ratio"))
    val u_time_hour_slot_ratio  = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_time_hour_slot_ratio"))
    //val u_time_dow_ratio  = split_uid_and_features(sc.textFile(
    // params.user_features_input_path + "u_time_dow_ratio"))
    //val u_time_workday_ratio = split_uid_and_features(sc.textFile(
     // params.user_features_input_path + "u_time_workday_ratio"))

    //val userAlias = sc.textFile(params.vipno2id_input_path)
    //val bUserMap = sc.broadcast(getMapData(userAlias))
/*
    val u_distinct_classes = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_distinct_classes"))
    //val u_class_ratio  = split_uid_and_features(sc.textFile(
    // params.user_features_input_path + "u_class_ratio"))
    //val u_classes_top5 = split_uid_and_features(sc.textFile(
    // params.user_features_input_path + "u_classes_top5"))
    val u_trend = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "u_trend"))
*/
    //step4
    /*
    val user_basket_money_sta  = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "user_basket_money_sta"))
    val user_log_money_sta = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "user_log_money_sta"))
    val user_frequency_year = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "user_frequency_year"))
    val user_frequency_month = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "user_frequency_month"))
    val user_frequency_day = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "user_frequency_day"))
    val user_money_year = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "user_money_year"))
    val user_money_month = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "user_money_month"))
    val user_money_day = split_uid_and_features(sc.textFile(
      params.user_features_input_path + "user_money_day"))
      */

    val basic_features = u_each_item_count_sta.union(u_total_sessions_rank_percent)
      .union(u_item_price_sta).union(u_total_money_rank_percent)
      .union(u_basket_size_sta).union(u_basket_money_slot_percent).union(u_order_days_ratio)
      .union(u_time_hour_slot_ratio)//.union(u_time_workday_ratio)  u_time_workday_ratio数据异常，部分用户缺失
      //.union(u_distinct_classes).union(u_trend)
      //.union(user_basket_money_sta).union(user_log_money_sta).union(user_frequency_year)
      //.union(user_frequency_month).union(user_frequency_day).union(user_money_year)
      //.union(user_money_month).union(user_money_day)
      .groupByKey().mapValues(_.mkString("|"))

    basic_features
  }



  def split_uid_and_features(rdd: RDD[String]): RDD[(String, String)] = {
    val splited_rdd = rdd.map {
      line =>
        val first_separator_index = line.indexOf("|")
        val uid = line.substring(0, first_separator_index)
        val features  = line.substring(first_separator_index + 1)
        (uid, features)
    }
    splited_rdd
  }

  def get_feature_minHashLsh(spark: SparkSession, user_features: RDD[(String, String)]) = {
    import org.apache.spark.ml.linalg.Vectors
    //val data = List(("1", "0.5|0.6|0.2"), ("3", "0.2|0.1|0.3"), ("2", "0.3|0.1|0.4"))
    val feature_Matrix = user_features.map {
      case (user_id, features) =>
        val fea = features.split("\\|", -1)
        (user_id, fea)
    }.map(x => (x._1, Vectors.dense(x._2.map(_.toDouble))))
    val feature_df = spark.createDataFrame(feature_Matrix).toDF("id", "features")
    feature_df.show()
/*
    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = mh.fit(feature_df)
    val transformed_feature_df = model.transform(feature_df)
    transformed_feature_df.show(false)
*/
    val brp_lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = brp_lsh.fit(feature_df)
    val transformed_feature_df = model.transform(feature_df)
    transformed_feature_df.show(false)

    val sim = model.approxSimilarityJoin(transformed_feature_df, transformed_feature_df, 10.0, "EuclideanDistance")
      .select(col("datasetA.id").alias("user_1"),
        col("datasetB.id").alias("user_2"),
        col("EuclideanDistance")).filter("user_1 != user_2")

    val sim_rdd = sim.rdd.map(x => (x(0).toString, (x(1).toString, 1 / (1 + x(2).toString.toDouble))))
      .groupByKey().flatMapValues(x => x.toList.sortBy(-_._2).take(10)).map(x => (x._1, x._2._1, x._2._2))
    BaseUtils.print_rdd(sim_rdd.map(_.toString()), "sim_rdd")
    sim_rdd

  }

  def get_dataRating_minHashLsh(spark: SparkSession,
                                data_rating: RDD[(String, String, Double)],
                                hot_item_num: Int, sim_num: Int) = {
    import org.apache.spark.ml.linalg.Vectors
    val item_count = data_rating.map(_._2.toInt).max

    val hot_item = data_rating.map(x => (x._2, 1)).reduceByKey(_ + _)
      .sortBy(-_._2).take(hot_item_num).map(_._1)
    val data_rating_filtered = data_rating.filter(x => ! hot_item.contains(x._2))
    BaseUtils.print_rdd(data_rating_filtered.map(_.toString), "data_rating_filtered")

    val data_rating_matrix = data_rating_filtered.map (x => (x._1.toInt, (x._2.toInt, x._3)))
      .groupByKey().mapValues {
      x =>
        val features = x.toList.map {
          case (feature_index, value) =>
            (feature_index, value)
        }.sortBy(_._1)
        Vectors.sparse(item_count + 1, features)
    }.map(x => (x._1, x._2)).sortByKey()
    BaseUtils.print_rdd(data_rating_matrix.map(_.toString()), "data_rating_minHashLsh_matrix")
    val data_rating_df = spark.createDataFrame(data_rating_matrix).toDF("id", "features")
    //data_rating_df.show()

    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = mh.fit(data_rating_df)
    val transformed_data_rating = model.transform(data_rating_df)
    transformed_data_rating.show(false)

    val sim = model.approxSimilarityJoin(transformed_data_rating, transformed_data_rating, 0.5, "JaccardDistance")
      .select(col("datasetA.id").alias("user_1"),
      col("datasetB.id").alias("user_2"),
      col("JaccardDistance")).filter("user_1 != user_2")

    val sim_rdd = sim.rdd.map(x => (x(0).toString, (x(1).toString, 1 - x(2).toString.toDouble)))
      .groupByKey().flatMapValues(x => x.toList.sortBy(-_._2).take(10)).map(x => (x._1, x._2._1, x._2._2))
    sim_rdd
  }


  def merge_user_sim(sc: SparkContext, u_feature_sim_path: String, u_rating_sim_path: String) = {
    val u_feature_sim = get_data_rating(sc, u_feature_sim_path)
    BaseUtils.print_rdd(u_feature_sim.map(_.toString()), "u_feature_sim")
    val u_rating_sim = get_data_rating(sc, u_rating_sim_path)
    BaseUtils.print_rdd(u_rating_sim.map(_.toString()), "u_rating_sim")
    val merged_sim = u_feature_sim.union(u_rating_sim).map(x => ((x._1, x._2), x._3))
      .groupByKey().mapValues {
      x =>
        val size = x.size

    }
  }

  def remove_existed_preprocess_dirs(params: Params): Unit = {
  }

  def main(args: Array[String]): Unit ={

    val defaultParams = Params()
    val parser = new OptionParser[Params]("preprocess"){
      opt[String]("data_rating_input_path")
        .action((x, c) => (c.copy(data_rating_input_path = x)))
      opt[String]("user_features_input_path")
        .action((x, c) => (c.copy(user_features_input_path = x)))
      opt[String]("user_eda_features_input_path")
        .action((x, c) => (c.copy(user_eda_features_input_path = x)))

      checkConfig(params => success)
    }

    parser.parse(args, defaultParams).map{
      params => run(params)
    }.getOrElse{
      System.exit(1)
    }

  }

  case class Params(
                     data_rating_input_path: String = "/home/chroot/test_data/etl/train_implicit",
                     user_features_input_path: String = "/home/chroot/test_data/u_feature/",
                     user_eda_features_input_path: String = "/home/chroot/test_data/u_feature/"
                   )
}
