package mapreduce.userrelateds;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRelatedsMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] line = value.toString().split("\t");

		String productId = line[1];
		String userId = line[2];
		Integer score = new Integer(line[6]);
		if(score.intValue() > 3)
			context.write(new Text(productId), new Text(userId));
	}
}
