package org.example.benchmark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FileChannelReadBenchmark {

	private static final String FILE_PATH ="./files/targetFile-1GB.txt";

	private static RandomAccessFile targetFile;

	private final ByteBuffer buffer_1kb = ByteBuffer.allocate(1024);
	private final ByteBuffer buffer_2kb = ByteBuffer.allocate(2*1024);
	private final ByteBuffer buffer_4kb = ByteBuffer.allocate(4*1024);
	private final ByteBuffer buffer_32kb = ByteBuffer.allocate(32*1024);
	private final ByteBuffer buffer_64kb = ByteBuffer.allocate(64*1024);
	private final ByteBuffer buffer_1mb = ByteBuffer.allocate(1024*1024);
	private final ByteBuffer buffer_2mb = ByteBuffer.allocate(2*1024*1024);
	private final ByteBuffer buffer_4mb = ByteBuffer.allocate(4*1024*1024);
	private final ByteBuffer buffer_32mb = ByteBuffer.allocate(32*1024*1024);
	private final ByteBuffer buffer_64mb = ByteBuffer.allocate(64*1024*1024);
	// odd1 = 1mb -1b
	private final ByteBuffer buffer_odd1 = ByteBuffer.allocate((1024*1024)-1);
	// odd2 = 1mb -4kb
	private final ByteBuffer buffer_odd2 = ByteBuffer.allocate(1020*1024);


	@Setup(Level.Invocation)
	public void setup() throws FileNotFoundException {
		targetFile = new RandomAccessFile(FILE_PATH, "r");
	}

	@TearDown(Level.Invocation)
	public void tearDown() throws IOException {
		targetFile.close();
	}

	@Benchmark
	public long FileChannelRead_1kb() throws IOException {
		return fcRead(targetFile, buffer_1kb);
	}

	@Benchmark
	public long FileChannelRead_2kb() throws IOException {
		return fcRead(targetFile, buffer_2kb);
	}

	@Benchmark
	public long FileChannelRead_4kb() throws IOException {
		return fcRead(targetFile, buffer_4kb);
	}

	@Benchmark
	public long FileChannelRead_32kb() throws IOException {
		return fcRead(targetFile, buffer_32kb);
	}

	@Benchmark
	public long FileChannelRead_64kb() throws IOException {
		return fcRead(targetFile, buffer_64kb);
	}

	@Benchmark
	public long FileChannelRead_1mb() throws IOException {
		return fcRead(targetFile, buffer_1mb);
	}

	@Benchmark
	public long FileChannelRead_2mb() throws IOException {
		return fcRead(targetFile, buffer_2mb);
	}

	@Benchmark
	public long FileChannelRead_4mb() throws IOException {
		return fcRead(targetFile, buffer_4mb);
	}

	@Benchmark
	public long FileChannelRead_32mb() throws IOException {
		return fcRead(targetFile, buffer_32mb);
	}

	@Benchmark
	public long FileChannelRead_64mb() throws IOException {
		return fcRead(targetFile, buffer_64mb);
	}

	@Benchmark
	public long FileChannelRead_odd1() throws IOException {
		return fcRead(targetFile, buffer_odd1);
	}

	@Benchmark
	public long FileChannelRead_odd2() throws IOException {
		return fcRead(targetFile, buffer_odd2);
	}

	private long fcRead(RandomAccessFile targetFile, ByteBuffer buffer) throws IOException {
		FileChannel channel = targetFile.getChannel();
		while (channel.position() < channel.size()) {
			channel.read(buffer);
			buffer.clear();
		}
		return channel.position();
	}
	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(FileChannelReadBenchmark.class.getSimpleName())
				.forks(1)
				.build();
		new Runner(opt).run();
	}

}
