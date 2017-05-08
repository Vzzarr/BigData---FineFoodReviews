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
		Map<String, String> mapIduserKey_id = new TreeMap<>();
		for (Text value : values){
			int control = Integer.parseInt(value.toString().split("\\|")[1]); 
			if(control>=4){
				String idUserKey = value.toString().split("\\|")[0];
				String idUserId = value.toString().split("\\|")[0];
				mapIduserKey_id.put(idUserKey, idUserId);
			}
		}

		List<String> listUsers = new LinkedList<>(mapIduserKey_id.keySet());
		for (int i = 0; i<listUsers.size(); i++) {
			for(int j = i+1; j<listUsers.size(); j++){
				String coupleUsers = listUsers.get(i)+"|"+listUsers.get(j);
				add_productCoupleUsers(coupleUsers, key.toString());
			}
		}
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

	private void add_productCoupleUsers(String usersCouple, String products){
		if(!mapCoupleUsers2productsIdList.containsKey(usersCouple)){
			List<String> scores = new LinkedList<>();
			scores.add(products);
			mapCoupleUsers2productsIdList.put(usersCouple, scores);
		}else 
			mapCoupleUsers2productsIdList.get(usersCouple).add(products);
	}
}
