package mapreduce.userfavouriteproducts;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserFavouriteProductsMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] line = value.toString().split("\t");
		String userId = line[2];
		String productId = line[1];
		int score = Integer.parseInt(line[6]);

		context.write(new Text(userId), new Text(productId + "|" + score));
	}
}
