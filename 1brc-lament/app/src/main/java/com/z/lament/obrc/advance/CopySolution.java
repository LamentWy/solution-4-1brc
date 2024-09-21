package com.z.lament.obrc.advance;


import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import sun.misc.Unsafe;

/**
 * A copy of CalculateAverage_thomaswue's final version.
 * <p>
 * originCode: <a href="https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java">here</a>
 *
 * */
public class CopySolution {

	private static final String PATH = "./measurements.txt";
	private static final int MAX_NAMES = 10000;
	private static final int MAX_NAME_LENGTH = 120;
	private static final int HASH_TABLE_SIZE = 1 << 17; // 128K
	private static final int SEGMENT_SIZE_2MB = 1 << 21;
	private static final int TEMP_MIN = -999;
	private static final int TEMP_MAX = 999;

	/* --- these parts are for debug & test --- */
	private static final int SEGMENT_SIZE_32MB = 1 << 25; // same level with 2MB
	private static final int SEGMENT_SIZE_4KB = 1 << 12; // even my page size is 4KB, it's MUCH slower than 2MB

	private static final AtomicInteger collisionCounter = new AtomicInteger(0);

	public static void foundCollision() {
		collisionCounter.incrementAndGet();
	}

	public static void printCollisions() {
		System.out.println("collision count: " + collisionCounter.get());
	}
	/* --- end --- */

	public static void main(String[] args) throws IOException, InterruptedException {

		/* 创建释放 map 文件的线程 ,注释掉该部分可以方便使用 idea 的 profiler */
		// Start worker subprocess if this process is not the worker.
		if (args.length == 0 || !("--worker".equals(args[0]))) {
			cleanupWorker();
			return;
		}
		/* --- end --- */

		long begin = System.currentTimeMillis();

		int numOfProcessers = Runtime.getRuntime().availableProcessors();

		// 读取文件
		try (FileChannel fileChannel = FileChannel.open(Path.of(PATH), StandardOpenOption.READ)) {

			long totalSize = fileChannel.size();
			long start = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, totalSize, Arena.global()).address();
			long end = start + totalSize;

			AtomicLong cursor = new AtomicLong(start);

			//并行处理
			Thread[] threads = new Thread[numOfProcessers];
			List<Data>[] allResults = new List[numOfProcessers];

			//创建并启动线程
			for (int i = 0; i < numOfProcessers; i++) {
				final int index = i;
				threads[i] = new Thread(() -> {
					List<Data> results = new ArrayList<>(MAX_NAMES);
					parseLoop(cursor, start, end, results);
					allResults[index] = results;
				});

				threads[i].start();
			}

			for (Thread thread : threads) {
				thread.join();
			}

			// 打印结果
			System.out.println(accumulateResults(allResults));
			// time cost
			System.out.printf("time cost: %s ms \n",System.currentTimeMillis() - begin);
			// 冲突次数统计
			printCollisions();
			System.out.close();
		}

	}

	private static TreeMap<String, Data> accumulateResults(List<Data>[] allResults) {
		TreeMap<String, Data> result = new TreeMap<>();
		for (List<Data> results : allResults) {
			for (Data d : results) {
				Data current = result.putIfAbsent(d.getStrName(), d);
				if (current != null) {
					current.accumulate(d);
				}
			}
		}
		return result;
	}

	private static final class Data {

		long namePart1, namePart2;
		long nameAddr;
		int min, max;
		int sum;
		int count;

		Data() {
			this.min = TEMP_MAX;
			this.max = TEMP_MIN;
		}

		@Override
		public String toString() {
			return String.format("%s/%s/%s", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
		}

		// 根据地址获取 name 并创建字符串
		public String getStrName() {
			MemoryReader reader = new MemoryReader(nameAddr, nameAddr + MAX_NAME_LENGTH);
			int nameLength = 0;
			while (reader.getByteAt(nameAddr + nameLength) != ';') {
				nameLength++;
			}
			byte[] nameBytes = new byte[nameLength];

			for (int i = 0; i < nameLength; i++) {
				nameBytes[i] = reader.getByteAt(nameAddr + i);
			}
			return new String(nameBytes, StandardCharsets.UTF_8);
		}

		public void accumulate(Data other) {
			min = Math.min(min, other.min);
			max = Math.max(max, other.max);
			sum += other.sum;
			count += other.count;
		}
	}

	private static class MemoryReader {

		private static final Unsafe UNSAFE = initUnsafe();

		private long currentAddress;
		private final long endAddress;

		public MemoryReader(long start, long end) {
			currentAddress = start;
			endAddress = end;
		}

		private static Unsafe initUnsafe() {
			Field field = null;
			try {
				field = Unsafe.class.getDeclaredField("theUnsafe");
				field.setAccessible(true);
				return (Unsafe) field.get(Unsafe.class);
			}
			catch (NoSuchFieldException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		boolean hasRemaining() {
			return currentAddress < endAddress;
		}

		long getCurrentAddress() {
			return currentAddress;
		}

		long getLong() {
			return UNSAFE.getLong(currentAddress);
		}

		long getLongAt(long addr) {
			return UNSAFE.getLong(addr);
		}

		void add(long length) {
			currentAddress += length;
		}

		byte getByteAt(long addr) {
			return UNSAFE.getByte(addr);
		}
	}

	private static void parseLoop_3_parts(AtomicLong cursor, long start, long end, List<Data> results) {
		// 这个 Data 数组看成一个基于开放寻址实现的哈希表，也就是我们存 Data 的地方。
		Data[] hashMap = new Data[HASH_TABLE_SIZE];

		while (true) {
			long currentAddress = cursor.addAndGet(SEGMENT_SIZE_2MB) - SEGMENT_SIZE_2MB;

			if (currentAddress >= end) {
				return;
			}

			long segmentStart;
			if (currentAddress == start) {
				segmentStart = start;
			}
			else {
				segmentStart = nextLine(currentAddress) + 1;
			}
			long segmentEnd = nextLine(Math.min(currentAddress + SEGMENT_SIZE_2MB, end - 1));

			long dist = (segmentEnd - segmentStart) / 3;
			long midPoint1 = nextLine(segmentStart + dist);
			long midPoint2 = nextLine(segmentStart + 2 * dist);

			MemoryReader reader1 = new MemoryReader(segmentStart, midPoint1);
			MemoryReader reader2 = new MemoryReader(midPoint1+1, midPoint2);
			MemoryReader reader3 = new MemoryReader(midPoint2+1, segmentEnd);

			while (true) {

				if (!reader1.hasRemaining()) {
					break;
				}
				if (!reader2.hasRemaining()) {
					break;
				}
				if (!reader3.hasRemaining()) {
					break;
				}

				long name1_p1 = reader1.getLong();
				long name2_p1 = reader2.getLong();
				long name3_p1 = reader3.getLong();
				long delimiterMask1 = findDelimiter(name1_p1);
				long delimiterMask2 = findDelimiter(name2_p1);
				long delimiterMask3 = findDelimiter(name3_p1);

				long name1_p2 = reader1.getLongAt(reader1.getCurrentAddress() + 8);
				long name2_p2 = reader2.getLongAt(reader2.getCurrentAddress() + 8);
				long name3_p2 = reader3.getLongAt(reader3.getCurrentAddress() + 8);
				long delimiterMask1_2 = findDelimiter(name1_p2);
				long delimiterMask2_2 = findDelimiter(name2_p2);
				long delimiterMask3_2 = findDelimiter(name3_p2);

				Data existingData1 = findData(name1_p1, delimiterMask1, name1_p2, delimiterMask1_2, reader1, hashMap, results);
				Data existingData2 = findData(name2_p1, delimiterMask2, name2_p2, delimiterMask2_2, reader2, hashMap, results);
				Data existingData3 = findData(name3_p1, delimiterMask3, name3_p2, delimiterMask3_2, reader3, hashMap, results);

				int tempInt1 = findNumber(reader1);
				int tempInt2 = findNumber(reader2);
				int tempInt3 = findNumber(reader3);

				storeTemp(existingData1, tempInt1);
				storeTemp(existingData2, tempInt2);
				storeTemp(existingData3, tempInt3);
			}

			// 前面三个 reader 任意一个都可能提前终止循环,所以这里要完成收尾
			while (reader1.hasRemaining()) {
				long nameP1 = reader1.getLong();
				long delimiterMask = findDelimiter(nameP1);
				long nameP2 = reader1.getLongAt(reader1.getCurrentAddress() + 8);
				long delimiterMask2 = findDelimiter(nameP2);
				storeTemp(findData(nameP1, delimiterMask, nameP2, delimiterMask2, reader1, hashMap, results), findNumber(reader1));
			}
			while (reader2.hasRemaining()) {
				long nameP1 = reader2.getLong();
				long delimiterMask = findDelimiter(nameP1);
				long nameP2 = reader2.getLongAt(reader2.getCurrentAddress() + 8);
				long delimiterMask2 = findDelimiter(nameP2);
				storeTemp(findData(nameP1, delimiterMask, nameP2, delimiterMask2, reader2, hashMap, results), findNumber(reader2));
			}
			while (reader3.hasRemaining()) {
				long nameP1 = reader3.getLong();
				long delimiterMask = findDelimiter(nameP1);
				long nameP2 = reader3.getLongAt(reader3.getCurrentAddress() + 8);
				long delimiterMask2 = findDelimiter(nameP2);
				storeTemp(findData(nameP1, delimiterMask, nameP2, delimiterMask2, reader3, hashMap, results), findNumber(reader3));
			}
		}
	}

	private static void parseLoop(AtomicLong cursor, long start, long end, List<Data> results) {

		// 这个 Data 数组看成一个基于开放寻址实现的哈希表，也就是我们存 Data 的地方。
		Data[] hashMap = new Data[HASH_TABLE_SIZE];

		while (true) {
			long currentAddress = cursor.addAndGet(SEGMENT_SIZE_2MB) - SEGMENT_SIZE_2MB;

			if (currentAddress >= end) {
				return;
			}

			long segmentStart;
			if (currentAddress == start) {
				segmentStart = start;
			}
			else {
				segmentStart = nextLine(currentAddress) + 1;
			}

			long segmentEnd = nextLine(Math.min(currentAddress + SEGMENT_SIZE_2MB, end - 1));

			MemoryReader reader = new MemoryReader(segmentStart, segmentEnd);

			while (reader.hasRemaining()) {
				long namePart1 = reader.getLong();
				long delimiterMask = findDelimiter(namePart1);
				long namePart2 = reader.getLongAt(reader.getCurrentAddress() + 8);
				long delimiterMask2 = findDelimiter(namePart2);
				Data existingData = findData(namePart1, delimiterMask, namePart2, delimiterMask2, reader, hashMap, results);
				int tempInt = findNumber(reader);
				storeTemp(existingData, tempInt);
			}
		}
	}

	private static void storeTemp(Data data, int tempInt) {
		data.min = Math.min(data.min, tempInt);
		data.max = Math.max(data.max, tempInt);
		data.sum += tempInt;
		data.count++;
	}

	private static int findNumber(MemoryReader reader) {
		long originNumber = reader.getLongAt(reader.getCurrentAddress() + 1);
		int decimalPos = Long.numberOfTrailingZeros(~originNumber & 0x10101000L);
		int tempInt = convert2Int(originNumber, decimalPos);
		reader.add((decimalPos >> 3) + 4l);
		return tempInt;
	}

	// from ascii to int without branches, created by Quan Anh Mai.
	// copy from Thomaswue's solution and made a little modification.
	private static int convert2Int(long numberWord, int decimalSepPos) {
		int shift = 28 - decimalSepPos;
		// signed is -1 if negative, 0 otherwise
		long signed = (~numberWord << 59) >> 63;
		long designMask = ~(signed & 0xFF);
		// Align the number to a specific position and transform the ascii to digit value
		long digits = ((numberWord & designMask) << shift) & 0x0F000F0F00L;
		// Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
		// 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
		// 0x000000UU00TTHH00 + 0x00UU00TTHH000000 * 10 + 0xUU00TTHH00000000 * 100
		long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
		return (int) ((absValue ^ signed) - signed);
	}


	private static final long[] MASK1 = new long[] {0xFFL, 0xFFFFL, 0xFFFFFFL, 0xFFFFFFFFL, 0xFFFFFFFFFFL, 0xFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL,
			0xFFFFFFFFFFFFFFFFL};
	private static final long[] MASK2 = new long[] {0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0xFFFFFFFFFFFFFFFFL};

	private static Data findData(long nameP1, long delimiterMask, long nameP2, long delimiterMaskB, MemoryReader reader, Data[] hashMap, List<Data> results) {

		Data existingData;
		long hash;
		long nameAddr = reader.getCurrentAddress();

		if ((delimiterMask | delimiterMaskB) != 0) {
			int letterCount1 = Long.numberOfTrailingZeros(delimiterMask) >>> 3; // 1~8
			int letterCount2 = Long.numberOfTrailingZeros(delimiterMaskB) >>> 3; // 0~8
			long mask = MASK2[letterCount1];
			nameP1 = nameP1 & MASK1[letterCount1];
			nameP2 = nameP2 & mask & MASK1[letterCount2];
			hash = nameP1 ^ nameP2;
			existingData = hashMap[hash2Index(hash)];
			reader.add(letterCount1 + (letterCount2 & mask));

			if (existingData != null && existingData.namePart1 == nameP1 && existingData.namePart2 == nameP2) {
				return existingData;
			}
		}
		else { // slow path when delimiter ";" is not in the first 16 bytes

			hash = nameP1 ^ nameP2;
			reader.add(16);
			while (true) {
				nameP1 = reader.getLong();
				delimiterMask = findDelimiter(nameP1);
				if (delimiterMask != 0) {
					int zeros = Long.numberOfTrailingZeros(delimiterMask);
					nameP1 = (nameP1 << (63 - zeros));
					reader.add(zeros >>> 3);
					hash ^= nameP1;
					break;
				}
				else {
					reader.add(8);
					hash ^= nameP1;
				}
			}
		}

		int nameLength = (int) (reader.getCurrentAddress() - nameAddr);

		int index = hash2Index(hash);

		while (true) {
			existingData = hashMap[index];
			if (existingData == null) {
				existingData = putIntoMap(hashMap, nameAddr, index, nameLength, reader, results);
			}
			// deal with collision
			int i = 0;
			for (; i < (nameLength + 1 - 8); i += 8) {
				if (reader.getLongAt(existingData.nameAddr + i) != reader.getLongAt(nameAddr + i)) {
					// collision found
                    foundCollision(); // this for debug
					index = (index + 31) & (HASH_TABLE_SIZE - 1);
					break;
				}
			}

			if (i < nameLength + 1 - 8){
				continue;
			}

			int remainingShift = (64 - ((nameLength + 1 - i) << 3));
			long unShiftSign = reader.getLongAt(existingData.nameAddr + i) ^ reader.getLongAt(nameAddr + i);
			if ((unShiftSign << remainingShift) == 0) {
				break;
			}
			else {
				// collision found
				foundCollision(); // this for debug
				index = (index + 31) & (HASH_TABLE_SIZE - 1);
			}
		}
		return existingData;
	}

	/**
	 * put data into hashmap and add to result list
	 * @return the data
	 */
	private static Data putIntoMap(Data[] hashMap, long nameAddr, int index, int nameLength, MemoryReader reader, List<Data> results) {
		Data result = new Data();
		hashMap[index] = result;
		int totalLength = nameLength + 1;
		result.namePart1 = reader.getLongAt(nameAddr);
		result.namePart2 = reader.getLongAt(nameAddr + 8);
		if (totalLength <= 8) {
			result.namePart1 = result.namePart1 & MASK1[nameLength];
			result.namePart2 = 0;
		}
		else if (totalLength < 16) {
			result.namePart2 = result.namePart2 & MASK1[nameLength - 8];
		}

		result.nameAddr = nameAddr;
		results.add(result);
		return result;
	}

	/**
	 * convert hash(long) to int then count the index in hashmap
	 * @return the index in hashmap
 	 * */
	private static int hash2Index(long hash) {
		long hashAsInt = hash ^ (hash >>> 33) ^ (hash >>> 15);
		return (int) (hashAsInt & (HASH_TABLE_SIZE - 1));
	}


	private static long findDelimiter(long raw) {
		long input = raw ^ 0x3B3B3B3B3B3B3B3BL;
		return (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
	}


	private static long nextLine(long prevAddress) {
		while (true) {
			long currentName = MemoryReader.UNSAFE.getLong(prevAddress);
			long input = currentName ^ 0x0A0A0A0A0A0A0A0AL;
			long pos = (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
			if (pos != 0) {
				prevAddress += Long.numberOfTrailingZeros(pos) >>> 3;
				break;
			}
			else {
				prevAddress += 8;
			}
		}
		return prevAddress;
	}


	private static void cleanupWorker() throws IOException {
		ProcessHandle.Info info = ProcessHandle.current().info();
		ArrayList<String> workerCommand = new ArrayList<>();
		info.command().ifPresent(workerCommand::add);
		info.arguments().ifPresent(args -> workerCommand.addAll(Arrays.asList(args)));
		workerCommand.add("--worker");
		new ProcessBuilder().command(workerCommand).inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE)
				.start().getInputStream().transferTo(System.out);
	}

}
