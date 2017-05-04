package mapreduce_bestproductmonth;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Product;

public class BestProductMonthReducer extends Reducer<Text, Text, Text, Text>{

	private List<Product> best5products = new LinkedList<>();
	private Map<String, List<Double>> product2scores = new HashMap<>();


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//		String products = "";

		for (Text value : values)
			add_product2scores(value.toString().split("\\|")[0], Double.parseDouble(value.toString().split("\\|")[1]));

		for (String productId : product2scores.keySet()) {
			Product product = new Product(productId,averageList(product2scores.get(productId)));
			addValue(product);//productAvg);
		}

		for(Product prodotto : best5products){
			//			products += prodotto.getIdProduct() + "\t" + prodotto.getAverage() + "\n";
			context.write(key, new Text(prodotto.getIdProduct() + "\t" + prodotto.getAverage()));
		}
	}

	private double averageList(List<Double> scores){
		double sumScores = 0;
		for (Double score : scores)
			sumScores += score;
		return (sumScores / scores.size());
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


	private void add_product2scores(String product, double score){
		if(!product2scores.containsKey(product)){
			List<Double> scores = new LinkedList<>();
			scores.add(score);
			product2scores.put(product, scores);
		}
		else
			product2scores.get(product).add(score);
	}
}
