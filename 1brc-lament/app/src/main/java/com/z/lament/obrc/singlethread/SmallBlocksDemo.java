package com.z.lament.obrc.singlethread;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * 单线程；
 * 文件切成小块,load 到堆中，lazy-load
 * <br/>
 * 耗时 2m 左右
 * */
public class SmallBlocksDemo {

	private static final String PATH = "./measurements.txt";

	static class Data {
		double min = Double.POSITIVE_INFINITY;
		double max = Double.NEGATIVE_INFINITY;
		double sum = 0.0;
		int count = 0;

		@Override
		public String toString() {
			return String.format("%s/%s/%s", min, Math.round((sum / count) * 10.0) / 10.0, max);
		}
		private static Data combine(Data data1, Data data2) {
			data1.min = Math.min(data1.min, data2.min);
			data1.max = Math.max(data1.max, data2.max);
			data1.sum = data1.sum + data2.sum;
			data1.count = data1.count + data2.count;
			return data1;
		}
	}

	static class BlockReader {

		byte[] buffer;
		long startPos;
		int size;
		RandomAccessFile file;

		int index = 0;
		boolean unUsed = true;


		BlockReader(long startPos, int size, RandomAccessFile file) {
			this.startPos = startPos;
			this.size = size;
			this.file = file;
		}

		private void load() {
			try {
				file.seek(startPos);
				file.read(buffer);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		boolean hasRemaining() {
			if (unUsed) {
				unUsed = false;
				this.buffer = new byte[size];
				load();
			}

			if (index < this.size) {
				return true;
			}
			else {
				this.buffer = null;
			}
			return false;
		}


		byte get() {
			return buffer[index++];
		}
	}

	private static Map<String, Data> parseMap(BlockReader buffer) {
		Map<String, Data> map = new HashMap<>(1 << 10); // 1024, avoid grow

		byte[] bytes = new byte[128];

		while (buffer.hasRemaining()) {
			int nameLength = 0;
			byte tmp = buffer.get();
			while (tmp != ';') {
				bytes[nameLength] = tmp;
				nameLength = nameLength + 1;
				tmp = buffer.get();
			}
			String name = new String(bytes, 0, nameLength, StandardCharsets.UTF_8);

			tmp = buffer.get(); // skip ';'

			// get temperature
			int tempIndex = 0; //using as index, last value from loop is length
			int ne = 1; // negative sign
			while (buffer.hasRemaining() && tmp != '\n') {
				if (tmp == '-') {
					ne = -1;
					// skip '-'
				}
				else if (tmp == '.') {
					// skip .
				}
				else {
					bytes[tempIndex] = tmp;
					tempIndex = tempIndex + 1;
				}

				tmp = buffer.get();
			}

			double temperature = parseDouble(bytes, tempIndex, ne);
			Data data = processData(name, temperature, map);
			map.put(name, data);
		}
		return map;

	}

	private static double parseDouble(byte[] bytes, int tempLength, int ne) {
		int n = 0;
		if (tempLength == 2) {
			n = (bytes[0] - 48) * 10 + (bytes[1] - 48);
		}
		else if (tempLength == 3) {
			n = (bytes[0] - 48) * 100 + (bytes[1] - 48) * 10 + (bytes[2] - 48);
		}
		return (n * ne / 10.0);
	}

	private static Data processData(String name, double temperature, Map<String, Data> map) {

		Data data = map.get(name);

		if (data == null) {
			data = new Data();
		}
		if (data.min > temperature) {
			data.min = temperature;
		}
		if (data.max < temperature) {
			data.max = temperature;
		}

		data.sum = data.sum + temperature;
		data.count = data.count + 1;
		return data;
	}

	private static List<BlockReader> divide(int blockNum) throws IOException {

		// create file
		List<BlockReader> buffers;
		RandomAccessFile file = new RandomAccessFile(new File(PATH), "r");
		buffers = new ArrayList<>();

		long totalSize = file.length();
		int blockSize = (int) (totalSize / blockNum);

		// 计算出每个 block 正确的 pos & size
		long headPos = 0;
		long tailPos = -1;
		int size = 0;

		for (int i = 0; i < blockNum; i++) {

			headPos = tailPos + 1;
			long limit = headPos + blockSize;
			boolean found = false;

			// find tail
			int n = 1;
			while (!found) {
				for (long tmpPos = limit - (8L * n); tmpPos < limit; tmpPos++) {
					file.seek(tmpPos);
					while (file.read() == '\n') {
						tailPos = tmpPos;
						found = true;
					}
				}
				n = n + 1;
			}

			size = (int) (tailPos - headPos);

			if (i == blockNum - 1) {
				size = (int) (totalSize - headPos);
			}

			BlockReader buffer = new BlockReader(headPos, size, file);
			buffers.add(buffer);
		}

		return buffers;
	}

	public static void process(String path) throws IOException {
		List<BlockReader> buffers = divide(200);
		Map<String, Data> resultMap = buffers.stream()
				.map(SmallBlocksDemo::parseMap)
				.flatMap(map -> map.entrySet().stream())
				.collect(Collectors.collectingAndThen(
						Collectors.toMap(Map.Entry::getKey,
								Map.Entry::getValue,
								Data::combine),
						TreeMap::new
				));
		System.out.println(resultMap);
	}

	public static void main(String[] args) throws IOException {
		process(PATH);
	}
}
