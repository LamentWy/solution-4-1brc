package org.example.benchmark;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/**
 * 对比 MappedByteBuffer 映射不同大小的文件时的读取速度。
 * 每次读取 4kb。
 * */

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MBBReadDiffSizeBenchmark {

	private static final int fileSize_1MB = 1024 * 1024;
	private static final String file_1MB = "./files/targetFile-1MB.txt";
	private static final int fileSize_128MB = 128 * 1024 * 1024;
	private static final String file_128MB = "./files/targetFile-128MB.txt";
	private static final int fileSize_512MB = 512 * 1024 * 1024;
	private static final String file_512MB = "./files/targetFile-512MB.txt";

	private RandomAccessFile targetFile;
	private MappedByteBuffer targetBuffer;

	private final ByteBuffer buffer_4kb = ByteBuffer.allocate(4*1024);
	@TearDown(Level.Invocation)
	public void tearDown() throws IOException {
		targetFile.close();
		targetBuffer.clear();
		targetBuffer = null;
		System.gc();
	}

	@Benchmark
	public long mfRead_1MB() throws IOException {
		targetFile = new RandomAccessFile(file_1MB, "r");
		targetBuffer = targetFile.getChannel().map(READ_ONLY, 0, fileSize_1MB);
		return mfRead(buffer_4kb);
	}

	@Benchmark
	public long mfRead_128MB() throws IOException {
		targetFile = new RandomAccessFile(file_128MB, "r");
		targetBuffer = targetFile.getChannel().map(READ_ONLY, 0, fileSize_128MB);
		return mfRead(buffer_4kb);
	}

	@Benchmark
	public long mfRead_512MB() throws IOException {
		targetFile = new RandomAccessFile(file_512MB, "r");
		targetBuffer = targetFile.getChannel().map(READ_ONLY, 0, fileSize_512MB);
		return mfRead(buffer_4kb);
	}

	private int mfRead(ByteBuffer buffer) {
		while (targetBuffer.hasRemaining()) {
			targetBuffer.get(buffer.array());
			buffer.clear();
		}
		return targetBuffer.position();
	}

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.include(MBBReadDiffSizeBenchmark.class.getSimpleName())
				.forks(1)
				.build();
		new Runner(options).run();
	}

}
