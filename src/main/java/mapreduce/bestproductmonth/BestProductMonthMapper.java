package mapreduce.bestproductmonth;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BestProductMonthMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] line = value.toString().split("\t");

		long unixTime = Long.parseLong(line[7]);
		String productId = line[1];
		double score = Double.parseDouble(line[6]);

		context.write(new Text(convertUnixTime(unixTime) + "|" + productId), new DoubleWritable(score));
	}
	
	private String convertUnixTime(long unixSeconds){
		Date date = new Date(unixSeconds*1000L);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		return sdf.format(date);
	}
}
