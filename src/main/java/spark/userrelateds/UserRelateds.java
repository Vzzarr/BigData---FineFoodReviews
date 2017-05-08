package spark.userrelateds;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class UserRelateds implements Serializable {

	private static final long serialVersionUID = 1L;
	private String inputFilePath;
	private String outputFolderPath;
	private static final int ID_USER = 2;
	private static final int ID_PRODUCT = 1;
	private static final int SCORE = 6;
	private Broadcast<Map<String, List<String>>> mapUsersRelatesdSame2productsIdList;

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
		mapUsersRelatesdSame2productsIdList = sc.broadcast(new TreeMap<>());
		JavaRDD<String> userRelateds = sc.textFile(inputFilePath);

		JavaPairRDD<String, String> idProduct2user =  userRelateds.mapToPair(row->{
			String[] tsvValues = row.split("\t");
			Integer scoreUp3 = new Integer(tsvValues[SCORE]);
			if(scoreUp3>3){
				return new Tuple2<>(tsvValues[ID_PRODUCT],tsvValues[ID_USER]);
			}else
				return null;
			//Serve per eliminare i valori nulli
		}).filter(filter -> filter != null);


		/*
		 * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W))
		 * pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, 
		 * rightOuterJoin, and fullOuterJoin.*/
		idProduct2user.join(idProduct2user)
		.mapToPair(row->{
			String user1 = row._2._1;
			String user2 = row._2._2;
			if(!user1.equals(user2)){
				add_UsersRelatesdSame2productsIdList(row._2._1+"|"+row._2._2,row._1);
				return new Tuple2<>(row._2._1+"|"+row._2._2,row._1);
			}else
				return null;
		})
		.filter(filter -> filter != null)
		.groupByKey()
		.sortByKey()
		.filter(filter -> checkValue(filter._1))
		.saveAsTextFile(outputFolderPath);

		sc.close();
		sc.stop();
	}

	private boolean checkValue(String key){
		boolean valuesIsUp2 = false;
		if(mapUsersRelatesdSame2productsIdList.getValue().get(key)!=null)
			if(mapUsersRelatesdSame2productsIdList.getValue().get(key).size()>2)
				valuesIsUp2 = true;
		return mapUsersRelatesdSame2productsIdList.getValue().containsKey(key)&&valuesIsUp2;
	}

	private void add_UsersRelatesdSame2productsIdList(String usersCouple, String products){
		//Controllo per vedere se il suo omologo inverso è già presente nella lista
		//in caso positivo non la aggiungo nella mappa
		String firstUser = usersCouple.split("\\|")[0];
		String secondUser = usersCouple.split("\\|")[1];
		String omologo = secondUser+"|"+firstUser;
		if(firstUser.compareTo(secondUser)!=0){
			if(!mapUsersRelatesdSame2productsIdList.getValue().containsKey(omologo)&&!mapUsersRelatesdSame2productsIdList.getValue().containsKey(usersCouple)){
				List<String> scores = new LinkedList<>();
				scores.add(products);
				mapUsersRelatesdSame2productsIdList.getValue().put(usersCouple, scores);
				//				System.out.println(mapCoupleUsers2productsIdList.get(usersCouple).get(0));
			}else if(mapUsersRelatesdSame2productsIdList.getValue().containsKey(usersCouple)){
				if(!mapUsersRelatesdSame2productsIdList.getValue().get(usersCouple).contains(products))
					mapUsersRelatesdSame2productsIdList.getValue().get(usersCouple).add(products);
			}
			else{
				if(!mapUsersRelatesdSame2productsIdList.getValue().get(omologo).contains(products))
					mapUsersRelatesdSame2productsIdList.getValue().get(omologo).add(products);
			}
		}
	}

}