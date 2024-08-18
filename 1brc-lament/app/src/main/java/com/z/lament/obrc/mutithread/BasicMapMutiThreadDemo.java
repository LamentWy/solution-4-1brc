package com.z.lament.obrc.mutithread;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;


/**
 * 多线程版
 * */
public class BasicMapMutiThreadDemo {

	private static final String PATH = "./measurements.txt";

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
				synchronized (file) {
					file.seek(startPos);
					file.read(buffer);
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		boolean hasRemaining() {
			if (this.unUsed) {
				this.unUsed = false;
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

	static class BasicMap {
		Entry[] table = new Entry[1 << 11]; //
		int conflictCounter = 0;
		private static final int OFFSET_BASIS_32BIT = 0x811c9dc5;
		private static final int FNV_PRIME_32BIT = 0x01000193;


		static class Entry {
			byte[] key; // FIXED SIZE 32
			int keyLength; // real length
			Data data;
			Entry next;

			Entry(byte[] key, int keyLength, Data data) {
				this.key = key;
				this.keyLength = keyLength;
				this.data = data;
			}

			static class Data {
				int min;
				int max;
				int sum;
				int count;

				Data(int temperature) {
					this.min = temperature;
					this.max = temperature;
					this.sum = temperature;
					this.count = 1;
				}

				@Override
				public String toString() {
					return String.format("%s/%s/%s", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
				}
			}
		}

		static int hash(byte[] key) {
			int h;
			return (h = Arrays.hashCode(key)) ^ (h >>> 16);
		}

		static int fnv1a32Hash(byte[] key, int keyLength) {
			int h = OFFSET_BASIS_32BIT;
			for (int i = 0; i < keyLength; i++) {
				h = (h ^ key[i]) * FNV_PRIME_32BIT;
			}
			return h;
		}

		public void put(byte[] key, int keyLength, Entry.Data value) {

			int hash = fnv1a32Hash(key, keyLength);
			int index = hash & (table.length - 1);
			Entry entry = table[index];

			if (null == entry) {
				table[index] = new Entry(key, keyLength, value);
			}
			else {
				if (isSameKey(key, keyLength, entry)) {
					entry.data.min = Math.min(entry.data.min, value.min);
					entry.data.max = Math.max(entry.data.max, value.max);
					entry.data.sum = entry.data.sum + value.sum;
					entry.data.count++;
				}
				else {
					boolean find = false;
					while (entry.next != null) {
						entry = entry.next;
						if (isSameKey(key, keyLength, entry)) {
							entry.data.min = Math.min(entry.data.min, value.min);
							entry.data.max = Math.max(entry.data.max, value.max);
							entry.data.sum = entry.data.sum + value.sum;
							entry.data.count++;
							find = true;
							break;
						}
					}
					if (!find) {
						conflictCounter++;
						entry.next = new Entry(key, keyLength, value);
					}
				}
			}
		}

		private static boolean isSameKey(byte[] key, int keyLength, Entry entry) {
			return keyLength == entry.keyLength && eq(key, entry.key);
			//return keyLength == entry.keyLength && Arrays.equals(key, entry.key);
		}


		private static boolean eq(byte[] a, byte[] b) {

			ByteVector byteVector1 = ByteVector.fromArray(ByteVector.SPECIES_256, a, 0);
			ByteVector byteVector2 = ByteVector.fromArray(ByteVector.SPECIES_256, b, 0);
			VectorMask<Byte> eq = byteVector1.eq(byteVector2);

			int misIdx = eq.not().firstTrue();

			return misIdx == 32;

			//return eq.allTrue();
		}


		public Entry.Data get(byte[] key) {
			Entry entry = table[hash(key) & (table.length - 1)];
			while (!eq(key, entry.key)) {
				entry = entry.next;
			}
			return entry.data;
		}

	}

	private static void process(String path, int blockNum) throws IOException {
		List<BlockReader> buffers = divide(path, blockNum);

		List<BasicMap> basicMapList = buffers.stream()
				.parallel()
				.map(BasicMapMutiThreadDemo::parseMap)
				.toList();


		Map<String, BasicMap.Entry.Data> result = new TreeMap<>();

		basicMapList.forEach(map -> {
			for (int i = 0; i < map.table.length; i++) {
				BasicMap.Entry entry = map.table[i];
				while (entry != null) {
					String key = new String(entry.key, StandardCharsets.UTF_8).trim();
					if (result.containsKey(key)){
						entry.data.min = Math.min(entry.data.min, result.get(key).min);
						entry.data.max = Math.max(entry.data.max, result.get(key).max);
						entry.data.sum = entry.data.sum + result.get(key).sum;
						entry.data.count = entry.data.count + result.get(key).count;

					}
					result.put(key,entry.data);
					entry = entry.next;
				}
			}
		});

		System.out.println(result);

	}

	private static List<BlockReader> divide(String path, int blockNum) throws IOException {

		List<BlockReader> buffers;
		RandomAccessFile file = new RandomAccessFile(new File(path), "r");
		buffers = new ArrayList<>();

		long totalSize = file.length();
		int blockSize = (int) (totalSize / blockNum);

		// 计算出每个 block 正确的 pos & size
		long headPos;
		long tailPos = -1;
		int size;

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

	private static BasicMap parseMap(BlockReader buffer) {

		BasicMap map = new BasicMap();

		byte[] bytes = new byte[128];
		byte[] nameBytes;
		while (buffer.hasRemaining()) {
			int nameLength = 0;
			byte tmp = buffer.get();
			while (tmp != ';') {
				bytes[nameLength] = tmp;
				nameLength = nameLength + 1;
				tmp = buffer.get();
			}
			nameBytes = new byte[32];
			System.arraycopy(bytes, 0, nameBytes, 0, nameLength);

			tmp = buffer.get(); // skip ';'

			// get temperature
			int tempIndex = 0; //using as index, last value from loop is length
			int ne = 1;
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

			int tempInt = parseInt(bytes, tempIndex, ne);

			BasicMap.Entry.Data data = new BasicMap.Entry.Data(tempInt);
			map.put(nameBytes, nameLength, data);
		}

		return map;
	}

	private static int parseInt(byte[] bytes, int tempLength, int ne) {
		int n = 0;
		if (tempLength == 2) {
			n = (bytes[0] - 48) * 10 + (bytes[1] - 48);
		}
		else if (tempLength == 3) {
			n = (bytes[0] - 48) * 100 + (bytes[1] - 48) * 10 + (bytes[2] - 48);
		}

		return n * ne;
	}

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		process(PATH, 400);
		long end = System.currentTimeMillis();
		System.out.printf("cost: %s ms%n", end - start);
	}
}
