package mapreduce.userrelateds;

import java.io.IOException;
import java.util.*;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRelatedsReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Set<String> setUserKey_id = new HashSet<>();
		for (Text value : values)
			setUserKey_id.add(value.toString());

		List<String> listUsers = new LinkedList<>(setUserKey_id);
		for (int i = 0; i<listUsers.size(); i++) {
			for(int j = i+1; j<listUsers.size(); j++){
				String coupleUsers = listUsers.get(i)+"|"+listUsers.get(j);
				context.write(new Text(coupleUsers), key);
			}
		}
	}
}
