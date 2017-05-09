package spark.userrelateds;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class UserRelateds implements Serializable {

	private static final long serialVersionUID = 1L;
	private String inputFilePath;
	private String outputFolderPath;
	private static final int ID_USER = 2;
	private static final int ID_PRODUCT = 1;
	private static final int SCORE = 6;

	public UserRelateds(String inFile, String outPath) {
		this.inputFilePath = inFile;
		this.outputFolderPath = outPath;
	}

	public static void main(String[] args) {
		double startTime = System.currentTimeMillis();

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		UserRelateds bpm = new UserRelateds(args[0], args[1]);
		bpm.run();

		double stopTime = System.currentTimeMillis();
		double elapsedTime = (stopTime - startTime) / 1000;
		System.out.println("TEMPO DI ESECUZIONE:\t" + elapsedTime + "s");
	}

	@SuppressWarnings({ "resource"})
	private void run() {
		SparkConf conf = new SparkConf().setAppName(this.getClass().getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> userRelateds = sc.textFile(inputFilePath);

		JavaPairRDD<String, String> idProduct2user =  userRelateds
				.filter(filter -> Integer.parseInt(filter.split("\t")[SCORE]) > 3)
				.mapToPair(row -> {
					String[] tsvValues = row.split("\t");
					return new Tuple2<>(tsvValues[ID_PRODUCT], tsvValues[ID_USER]);
				});

		//key v1, key v2 -> key(v1, v2)
		idProduct2user.join(idProduct2user).filter(filter -> filter._2._1.compareTo(filter._2._2) < 0)
		.mapToPair(row -> new Tuple2<>(row._2._1 + "|" + row._2._2, row._1)).groupByKey()
		.mapToPair(coupleUser_products -> {
			Set<String> products = new HashSet<>();
			for (String iterable_element : coupleUser_products._2)
				products.add(iterable_element);
			return new Tuple2<>(coupleUser_products._1, products);
		}).filter(filter -> filter._2.size() > 2).sortByKey().saveAsTextFile(outputFolderPath);

		sc.close();
		sc.stop();
	}
}