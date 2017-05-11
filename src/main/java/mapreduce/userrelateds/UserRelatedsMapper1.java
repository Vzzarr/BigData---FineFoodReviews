package mapreduce.userrelateds;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRelatedsMapper1 extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] line = value.toString().split("\t");

		String coupleUser = line[0];
		String productId = line[1];
		context.write(new Text(coupleUser), new Text(productId));
	}
}
