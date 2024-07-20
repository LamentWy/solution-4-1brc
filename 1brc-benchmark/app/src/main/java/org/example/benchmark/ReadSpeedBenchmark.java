package org.example.benchmark;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * 对比 MappedByteBuffer 和 FileChannel 顺序读取速度；
 * 目标文件大小 1GB，一次读 4kb。
 * */

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ReadSpeedBenchmark {

	private static final int BUFF_SIZE = 1024 * 4;
	private RandomAccessFile targetFile;
	private MappedByteBuffer targetFileBuffer;

	@Setup(Level.Invocation)
	public void setupTargetFile() throws IOException {
		targetFile = new RandomAccessFile("./files/targetFile-1GB.txt","r");
		targetFileBuffer = targetFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, targetFile.length());
	}

	@TearDown(Level.Invocation)
	public void tearDownTargetFile() throws IOException {
		targetFile.close();
		targetFileBuffer.clear();
		targetFileBuffer = null;
		System.gc();
	}

	@Benchmark
	public void FileChannelRead_4k(Blackhole blackhole) throws IOException {
		blackhole.consume(fcRead_4k());
		targetFile.close();
	}

	private long fcRead_4k() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(BUFF_SIZE);
		targetFile = new RandomAccessFile("./files/targetFile-1GB.txt","r");
		FileChannel channel = targetFile.getChannel();
		while (channel.position() < channel.size()) {
			channel.read(buffer);
			buffer.clear();
		}
		return channel.position();
	}

	@Benchmark
	public void mappedFileRead_4k(Blackhole blackhole) throws IOException {

		blackhole.consume(mfRead_4k(targetFileBuffer));
		targetFile.close();

	}

	@Benchmark
	public void mappedFileRead_WithLoad_4k(Blackhole blackhole) throws IOException {
		targetFileBuffer.load();
		blackhole.consume(mfRead_4k(targetFileBuffer));
		targetFile.close();
	}

	private long mfRead_4k(MappedByteBuffer mappedBuffer) {

		if (mappedBuffer.position() != 0){
			mappedBuffer.position(0);
		}
		ByteBuffer buffer = ByteBuffer.allocate(BUFF_SIZE);
		while (mappedBuffer.hasRemaining()){
			mappedBuffer.get(buffer.array());
			buffer.clear();
		}
		return mappedBuffer.position();
	}

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.include(ReadSpeedBenchmark.class.getSimpleName())
				.forks(1)
				.build();

		new Runner(options).run();
	}

}
