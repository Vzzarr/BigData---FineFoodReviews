package mapreduce.userrelateds;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRelatedsReducer extends Reducer<Text, Text, Text, Text>{

	private Map<String, List<String>> mapCoupleUsers2productsIdList = new TreeMap<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Map<String, String> mapIduser_score = new TreeMap<>();
		for (Text value : values){
			if(Integer.parseInt(value.toString().split("\\|")[1])>=4){
				String idUserKey = value.toString().split("\\|")[0];
				String idUserId = value.toString().split("\\|")[0];
				mapIduser_score.put(idUserKey, idUserId);
			}
		}

		for (String user1Id : mapIduser_score.keySet()) {
			for(String user2Id : mapIduser_score.keySet()){
				String coupleUsers = user1Id+"|"+user2Id;
				add_productCoupleUsers(coupleUsers, key.toString());
			}
		}
	}

	private void add_productCoupleUsers(String usersCouple, String products){
		//Controllo per vedere se il suo omologo è già presente nella lista
		//in caso positivo non la aggiungo nella mappa
		if(!mapCoupleUsers2productsIdList.containsKey(usersCouple.split("\\|")[1]+"|"+usersCouple.split("\\|")[0])){
			List<String> scores = new LinkedList<>();
			scores.add(products);
			mapCoupleUsers2productsIdList.put(usersCouple, scores);
		}
		else
			mapCoupleUsers2productsIdList.get(usersCouple).add(products);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		for (String coupleUsers : mapCoupleUsers2productsIdList.keySet()) {
			if(mapCoupleUsers2productsIdList.get(coupleUsers).size()>=3){
				String products = "";
				for (String idProduct : mapCoupleUsers2productsIdList.get(coupleUsers)) {
					products+= idProduct + " ";
				}
				context.write(new Text(coupleUsers), new Text(products));
			}
		}
	}

}