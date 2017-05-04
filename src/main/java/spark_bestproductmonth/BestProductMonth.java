package spark_bestproductmonth;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import utils.Product;
//import utils.Parse;
//import utils.Tweet;

public class BestProductMonth implements Serializable {

	private static String inputFilePath;
	private static String outputFolderPath;
	private static int unixTime = 7;

	//	private Map<String, List<Double>> product2scores = new HashMap<>();
//	private List<Product> best5products = new LinkedList<>();


	public BestProductMonth(String inFile, String outPath) {
		this.inputFilePath = inFile;
		this.outputFolderPath = outPath;
	}

	public static void main(String[] args) {

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		BestProductMonth bpm = new BestProductMonth(args[0], args[1]);
		bpm.run();
	}

	private void run() {
		SparkConf conf = new SparkConf().setAppName(this.getClass().getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFilePath);

		//		JavaRDD<String> data_product_score = data.map(line -> {
		//			String[] tsvValues = line.split("\t");
		//			return convertUnixTime(Long.parseLong(tsvValues[unixTime])) + "|" + tsvValues[1] + "|" + tsvValues[6];
		//		});

		JavaPairRDD<String, String> map = data.mapToPair(row -> {
			String[] tsvValues = row.split("\t");
			return new Tuple2<>(convertUnixTime(Long.parseLong(tsvValues[unixTime])), tsvValues[1] + "|" + tsvValues[6]);
		});

		Map<String, Iterable<String>> date_productScore = map.groupByKey().collectAsMap();
		Map<String, Iterable<String>> date_productScoreTree = new TreeMap<>();
		date_productScoreTree.putAll(date_productScore);

		for (String date : date_productScoreTree.keySet()) {
			Map<String, List<Double>> product2scores = new HashMap<>();
			List<Product> best5products = new LinkedList<>();

			date_productScore.get(date).forEach(product_score -> {
				String[] ps = product_score.split("\\|");

				if(!product2scores.containsKey(ps[0])){
					List<Double> scores = new LinkedList<>();
					scores.add(Double.parseDouble(ps[1]));
					product2scores.put(ps[0], scores);
				}
				else
					product2scores.get(ps[0]).add(Double.parseDouble(ps[1]));
			});
			
			for (String productId : product2scores.keySet()) {
				Product product = new Product(productId,averageList(product2scores.get(productId)));
				best5products = addValue(best5products, product);
			}
			for (Product product : best5products) {
				System.out.println(date + "\t" + product.getIdProduct() + "\t" + product.getAverage());
			}
		}

		//		JavaPairRDD<String, String> reduce = map.reduceByKey((a,b) -> {
		////			add_product2scores(a.split("\\|")[0], Double.parseDouble(a.split("\\|")[1]));
		//			add_product2scores(b.split("\\|")[0], Double.parseDouble(b.split("\\|")[1]));
		//
		//			for (String productId : product2scores.keySet()) {
		//				Product product = new Product(productId,averageList(product2scores.get(productId)));
		//				addValue(product);
		//			}
		//			String result = "";
		//			for (Product product : best5products) {
		//				result += (product.getIdProduct() + "\t" + product.getAverage() + "\n");
		//			}
		//			return result;
		//		});

		//		reduce.saveAsTextFile(outputFolderPath);


		//		JavaRDD<List<Tuple2<String,Tweet>>> hashtagTweetList = 
		//				tweets
		//				.map(tweet ->{
		//					String text = tweet.getText().replace("\n"," ");
		//					tweet.setText(text);
		//					
		//					List<String> words = Arrays.asList(text.split(" "));
		//					List<Tuple2<String,Tweet>> result = new ArrayList<>();
		//					Set<String> hashtags = new HashSet<String>();
		//					
		//					for(String word : words) {
		//						if(word.startsWith("#") && word.length() > 1) {
		//							hashtags.add(word);
		//						}
		//					}
		//						
		//					for(String hashtag : hashtags) {
		//						Tuple2<String,Tweet> item = new Tuple2<>(hashtag,tweet);
		//						result.add(item);
		//					}
		//				
		//					return result;
		//				});
		//		
		//		List<Tuple2<String,Tweet>> allHashtagTweetList = hashtagTweetList.reduce((x,y) -> {
		//			x.addAll(y);
		//			return x;
		//		});
		//		
		//		//JavaPairRDD<String,Tuple1<Tweet>> hashtagTweetPairRDD =
		//		JavaPairRDD<String,Tweet> hashtagTweetPairRDD = 
		//				sc.parallelize(allHashtagTweetList)
		//				.mapToPair(line -> {
		//					String key = line._1();
		//					Tweet value = line._2();
		//					
		//					//return new Tuple2<>(key,new Tuple1<>(value));
		//					return new Tuple2<>(key,value);
		//				});
		//		
		//		//JavaPairRDD<String,Iterable<Tuple1<Tweet>>> invertedIndex =
		//		JavaPairRDD<String,Iterable<Tweet>> invertedIndex =
		//				hashtagTweetPairRDD
		//				.groupByKey()
		//				.sortByKey();

		//		invertedIndex.saveAsTextFile(outputFolderPath);

		sc.close();
		sc.stop();
	}

	private String convertUnixTime(long unixSeconds){
		//		long unixSeconds = Long.parseLong(line[7]);
		Date date = new Date(unixSeconds*1000L);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		return sdf.format(date);
	}

	private List<Product> addValue(List<Product> best5products, Product product) {
		if (best5products.size() == 0)
			best5products.add(product);
		else if (best5products.get(0).getAverage() > product.getAverage())
			best5products.add(0, product);
		else if (best5products.get(best5products.size() - 1).getAverage() < product.getAverage())
			best5products.add(best5products.size(), product);
		else {
			int i = 0;
			while (best5products.get(i).getAverage() < product.getAverage()) {
				i++;
			}
			best5products.add(i, product);
		}
		if(best5products.size() > 5)
			best5products.remove(0);
		return best5products;
	}

//	private void add_product2scores(String productId, double score){
//		if(!product2scores.containsKey(productId)){
//			List<Double> scores = new LinkedList<>();
//			scores.add(score);
//			product2scores.put(productId, scores);
//		}
//		else
//			product2scores.get(productId).add(score);
//	}

	private double averageList(List<Double> scores){
		double sumScores = 0;
		for (Double score : scores)
			sumScores += score;
		return (sumScores / scores.size());
	}

}