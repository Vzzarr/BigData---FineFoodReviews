package spark.userfavouriteproducts;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import utils.Product;

public class UserFavouriteProducts implements Serializable {

	private static final long serialVersionUID = 1L;
	private String inputFilePath;
	private String outputFolderPath;
	private static final int ID_USER = 2;
	private static final int ID_PRODUCT = 1;
	private static final int SCORE = 6;

	public UserFavouriteProducts(String inFile, String outPath) {
		this.inputFilePath = inFile;
		this.outputFolderPath = outPath;
	}

	public static void main(String[] args) {

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		UserFavouriteProducts bpm = new UserFavouriteProducts(args[0], args[1]);
		bpm.run();
	}

	@SuppressWarnings({ "resource" })
	private void run() {
		SparkConf conf = new SparkConf().setAppName(this.getClass().getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFilePath);

		//key = idUser	values = idProduct_score
		data.mapToPair(row -> {
			String[] tsvValues = row.split("\t");
			return new Tuple2<>(tsvValues[ID_USER],
					new Tuple2<>(tsvValues[ID_PRODUCT], Double.parseDouble(tsvValues[SCORE])));	
			// select best 10 products for each user
		}).groupByKey().mapToPair(partitionUser -> {
			Iterator<Tuple2<String, Double>> it = partitionUser._2.iterator();
			List<Product> best10products = new LinkedList<>();
			while(it.hasNext()){
				Tuple2<String, Double> productId_score = it.next();
				Product product = new Product(productId_score._1);
				product.setScore(productId_score._2);
				best10products = addValue(best10products, product);
			}
			String result = " ";
			for (Product product : best10products)
				result += (product.getIdProduct() + "|" + product.getScore() + " ");
			return new Tuple2<>(partitionUser._1, result);
		}).sortByKey().saveAsTextFile(outputFolderPath);

		sc.close();
		sc.stop();
	}

	private List<Product> addValue(List<Product> best10products, Product product) {
		if (best10products.size() == 0)
			best10products.add(product);
		else if (best10products.get(0).getScore() > product.getScore())
			best10products.add(0, product);
		else if (best10products.get(best10products.size() - 1).getScore() < product.getScore())
			best10products.add(best10products.size(), product);
		else {
			int i = 0;
			while (best10products.get(i).getScore() < product.getScore()) {
				i++;
			}
			best10products.add(i, product);
		}
		if(best10products.size() > 10)
			best10products.remove(0);
		return best10products;
	}
}