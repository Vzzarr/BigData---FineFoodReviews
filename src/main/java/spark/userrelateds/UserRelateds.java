package spark.userrelateds;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import utils.Product;

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

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		UserRelateds bpm = new UserRelateds(args[0], args[1]);
		bpm.run();
	}

	@SuppressWarnings({ "resource", "serial" })
	private void run() {
		SparkConf conf = new SparkConf().setAppName(this.getClass().getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> userRelateds = sc.textFile(inputFilePath);

		JavaPairRDD<String, String> idProduct2user =  userRelateds.mapToPair(row->{
			String[] tsvValues = row.split("\t");
			Integer scoreUp3 = new Integer(tsvValues[SCORE]);
			if(scoreUp3>3){
				return new Tuple2<>(tsvValues[ID_PRODUCT],tsvValues[ID_USER]);
			}else
				return null;
			//Serve per eliminare i valori nulli
		}).filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				return v1 != null;
			}
		});


		/*
		 * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W))
		 * pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, 
		 * rightOuterJoin, and fullOuterJoin.*/
		idProduct2user.join(idProduct2user)
		.mapToPair(row->{
			String user1 = row._2._1;
			String user2 = row._2._2;
			if(!user1.equals(user2)){
				return new Tuple2<>(row._2._1+"|"+row._2._2,row._1);
			}else
				return null;
		})
		.filter(new Function<Tuple2<String,String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				return v1!=null;
			}
		})
		.groupByKey()
		.filter(new Function<Tuple2<String, Iterable<String>>, Boolean>(){

			@Override
			public Boolean call(Tuple2<String, Iterable<String>> v) throws Exception {
				// TODO Auto-generated method stub
				Iterator<String> it = v._2.iterator();
				int numberProducts = 0;
				while(it.hasNext()){
					it.next();
					numberProducts++;
				}
				return numberProducts>2;
			}

		})
		.saveAsTextFile(outputFolderPath);
		//		joined.mapToPair(row->{
		//			return new Tuple2<>(row._2, row._1);
		//		});
	}
}