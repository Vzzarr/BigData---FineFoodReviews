package mapreduce.bestproductmonth;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Product;

public class BestProductMonthReducer extends Reducer<Text, DoubleWritable, Text, Text>{

	private List<Product> best5products = new LinkedList<>();
	private Map<String, List<String>> date2idProduct_value = new TreeMap<>();

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		double avg = 0;
		double count = 0;
		for (DoubleWritable value : values){
			avg+=value.get();
			count++;
		}
		add_product2scores(key.toString().split("\\|")[0], key.toString().split("\\|")[1] + "|" + (avg/count));
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		for (String date : date2idProduct_value.keySet()) {
			best5products = new LinkedList<>();
			for (String idProduct_avg : date2idProduct_value.get(date)) {
				Product p = new Product(idProduct_avg.split("\\|")[0]);
				p.setAverage(Double.parseDouble(idProduct_avg.split("\\|")[1]));
				addValue(p);
			}
			context.write(new Text(date), new Text(getIdProducsAvg(best5products)));
		}
	}

	private String getIdProducsAvg(List<Product> products){
		String values = "";
		for (Product product : products) {
			values += product.getIdProduct() + "|" + product.getAverage() + " ";
		}
		return values;
	}

	private void addValue(Product product) {
		if (best5products.size() == 0)
			best5products.add(product);
		else if (best5products.get(0).getAverage() > product.getAverage())
			best5products.add(0, product);
		else if (best5products.get(best5products.size() - 1).getAverage() < product.getAverage())
			best5products.add(best5products.size(), product);
		else {
			int i = 0;
			while (best5products.get(i).getAverage() < product.getAverage()) {
				i++;
			}
			best5products.add(i, product);
		}

		if(best5products.size() > 5)
			best5products.remove(0);
	}

	private void add_product2scores(String product, String score){
		if(!date2idProduct_value.containsKey(product)){
			List<String> scores = new LinkedList<>();
			scores.add(score);
			date2idProduct_value.put(product, scores);
		}
		else
			date2idProduct_value.get(product).add(score);
	}
}