package com.z.lament.obrc.baseline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Baseline
 * */
public class MyBaseline {

	private static final String PATH = "./measurements.txt";

	record OriginRow(String name, double temperature) { }

	static class Data {
		double min = Double.POSITIVE_INFINITY;
		double max = Double.NEGATIVE_INFINITY;
		double sum = 0.0;
		int count = 0;
	}


	public static void main(String[] args) throws IOException {
		process();
	}

	private static void process() throws IOException {

		Stream<String> strStream = Files.lines(Paths.get(PATH));
		Map<String, String> hashMap = strStream
				.map(s -> {
					String[] row = s.split(";");
					return new OriginRow(row[0], Double.parseDouble(row[1]));
				})
				.collect(Collectors.groupingBy(or -> or.name, getCollector()));

		Map<String, String> resultMap = new TreeMap<>(hashMap);

		System.out.println(resultMap);
	}

	private static Collector<OriginRow/*input*/,
			Data/*the mutable accumulation type*/,
			String/* output*/> getCollector() {

		return Collector.of(
				Data::new/*Supplier*/,
				(data, o) -> {
					data.min = Math.min(data.min, o.temperature);
					data.max = Math.max(data.max, o.temperature);
					data.sum = data.sum + o.temperature;
					data.count = data.count + 1;
				}/* BiConsumer */,
				(data1, data2) -> {
					Data data = new Data();
					data.min = Math.min(data1.min, data2.min);
					data.max = Math.max(data1.max, data2.max);
					data.sum = data1.sum + data2.sum;
					data.count = data1.count + data2.count;
					return data;
				}/*BinaryOperator*/,
				data -> String.format("%s/%s/%s",
						data.min, Math.round((data.sum/data.count) * 10.0) / 10.0, data.max)
				/* finish function*/
		);
	}

}
