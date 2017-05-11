package mapreduce.userrelateds;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRelatedsReducer1 extends Reducer<Text, Text, Text, Text>{


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Set<String> products = new TreeSet<>(); 
		for (Text text : values) 
			products.add(text.toString());
		if(products.size()>2){
			String productsIds = "";
			for (String product : products) 
				productsIds += product + " ";
			context.write(key, new Text(productsIds));
		}
	}
}

