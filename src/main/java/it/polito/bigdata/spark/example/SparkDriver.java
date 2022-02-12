package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Int;

public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String outputPath;
		String prefix;
		
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

		// Create a configuration object and set the name of the application
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		/* * * * * * * /
		/* * Task 1 * */
		JavaRDD<String> filteredRDD = wordFreqRDD.filter(line -> line.startsWith(prefix));

		// print the nr of filtered lines
		System.out.println("number of filtered lines: "+filteredRDD.count());

		// select the max frequency among the filtered, first of all map string in integres
		// (personalized object WordOccurences
		JavaRDD<WordOccureces> frequencesRDD = filteredRDD.map(line -> {
			String[] splitted = line.split("\\s+");
			return new WordOccureces(splitted[0], Integer.valueOf(splitted[1]));
		});

		// select the maximum anche : Integer maxFreq = frequenciesRDD.reduce((v1, v2) -> Integer.max(v1, v2));
		WordOccureces maxObj = frequencesRDD.reduce((el1, el2)->{
			if(el1.getOccurencies()>el2.getOccurencies())
				return el1;
			else
				return el2;
		});

		int max = maxObj.getOccurencies();
		System.out.println("maximum occurences "+max);

		/* * * * * * * /
		/* * Task 2 * */
		// establish the frequences trashold (80% of max)
		double treshold = max*0.8;
		System.out.println("Treshold : "+treshold);

		//filter only those with freq higher than treshold
		// also..
		// Filter the lines with freq > 0.8*maxFreq
//		JavaRDD<String> filteredWordsFreqRDD = filteredWordsRDD
//				.filter(line -> Double.compare(Double.parseDouble(line.split("\t")[1]), 0.8 * maxFreq) > 0);

		JavaRDD<WordOccureces> overTreshRDD = frequencesRDD.filter(el ->
			el.getOccurencies()>treshold
		);

		System.out.println("New number of words over treshold "+overTreshRDD.count());

		overTreshRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
