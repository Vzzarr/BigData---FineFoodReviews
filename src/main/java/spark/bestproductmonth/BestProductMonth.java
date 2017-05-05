package spark.bestproductmonth;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import utils.Product;

public class BestProductMonth implements Serializable {

	private static final long serialVersionUID = 1L;
	private String inputFilePath;
	private String outputFolderPath;
	private static final int UNIXTIME = 7;
	private static final int ID_PRODUCT = 1;
	private static final int SCORE = 6;

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

	@SuppressWarnings({ "resource", "rawtypes", "unchecked" })
	private void run() {
		SparkConf conf = new SparkConf().setAppName(this.getClass().getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFilePath);

		//key = date_idProduct	values = score
		data.mapToPair(row -> {
			String[] tsvValues = row.split("\t");
			return new Tuple2<>(convertUnixTime(Long.parseLong(tsvValues[UNIXTIME])) + "|" + tsvValues[ID_PRODUCT] , 
					new Tuple2<>(Double.parseDouble(tsvValues[SCORE]), new Double(1.0)));	
			// avg score for each product in each month
		}).reduceByKey((a,b) ->  new Tuple2(a._1 + b._1, a._2 + b._2)).mapValues(tuple -> tuple._1 / tuple._2)
		//change map -> key = date	values = idProduct_score
		.mapToPair(row -> {
			String[] rowsValues = row._1.split("\\|");
			return new Tuple2<>(rowsValues[0], rowsValues[1] + "|" + row._2);
		})
		//select top k elements with avg score with method addValue() and print into file
		.groupByKey().mapToPair(partitionDate -> {
			Iterator<String> it = partitionDate._2.iterator();
			List<Product> best5products = new LinkedList<>();
			while(it.hasNext()){
				String[] id_avg = it.next().split("\\|");
				Product product = new Product(id_avg[0]);
				product.setAverage(Double.parseDouble(id_avg[1]));
				best5products = addValue(best5products, product);
			}
			String result = " ";
			for (Product product : best5products)
				result += (product.getIdProduct() + "|" + product.getAverage() + " ");
			return new Tuple2<>(partitionDate._1, result);
		}).sortByKey().saveAsTextFile(outputFolderPath);

		sc.close();
		sc.stop();
	}

	private String convertUnixTime(long unixSeconds){
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
}