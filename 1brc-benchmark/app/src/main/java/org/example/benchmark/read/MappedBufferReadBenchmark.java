package org.example.benchmark.read;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
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
public class MappedBufferReadBenchmark {
	private static final String FILE_PATH ="./files/targetFile-1GB.txt";

	private RandomAccessFile targetFile;
	private MappedByteBuffer targetBuffer;

	private final ByteBuffer buffer_32b = ByteBuffer.allocate(32);
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
	private final ByteBuffer buffer_128mb = ByteBuffer.allocate(128*1024*1024);
	private final ByteBuffer buffer_512mb = ByteBuffer.allocate(512*1024*1024);
	// odd1 = 1mb -1b
	private final ByteBuffer buffer_odd1 = ByteBuffer.allocate((1024*1024)-1);
	// odd2 = 1mb -4kb
	private final ByteBuffer buffer_odd2 = ByteBuffer.allocate(1020*1024);


	@Setup(Level.Invocation)
	public void setup() throws IOException {
		targetFile = new RandomAccessFile(FILE_PATH,"r");
		targetBuffer = targetFile.getChannel().map(FileChannel.MapMode.READ_ONLY,0,1024*1024*1024);
	}

	@TearDown(Level.Invocation)
	public void tearDown() throws IOException {
		targetFile.close();
		targetBuffer.clear();
		targetBuffer = null;
		System.gc();
	}

	@Benchmark
	public long mfRead_32b(){
		return mfRead(buffer_32b);
	}

	@Benchmark
	public long mfRead_1kb(){
		return mfRead(buffer_1kb);
	}

	@Benchmark
	public long mfRead_2kb(){
		return mfRead(buffer_2kb);
	}

	@Benchmark
	public long mfRead_4kb(){
		return mfRead(buffer_4kb);
	}

	@Benchmark
	public long mfRead_32kb(){
		return mfRead(buffer_32kb);
	}

	@Benchmark
	public long mfRead_64kb(){
		return mfRead(buffer_64kb);
	}

	@Benchmark
	public long mfRead_1mb(){
		return mfRead(buffer_1mb);
	}

	@Benchmark
	public long mfRead_2mb(){
		return mfRead(buffer_2mb);
	}

	@Benchmark
	public long mfRead_4mb(){
		return mfRead(buffer_4mb);
	}

	@Benchmark
	public long mfRead_32mb(){
		return mfRead(buffer_32mb);
	}

	@Benchmark
	public long mfRead_64mb(){
		return mfRead(buffer_64mb);
	}

	@Benchmark
	public long mfRead_128mb(){
		return mfRead(buffer_128mb);
	}

	@Benchmark
	public long mfRead_512mb(){
		return mfRead(buffer_512mb);
	}

	@Benchmark
	public long mfRead_odd1(){
		while (targetBuffer.hasRemaining()) {
			int left = targetBuffer.limit() - targetBuffer.position();
			if (left < buffer_odd1.limit()){
				targetBuffer.get(new byte[left]);
				continue;
			}
			targetBuffer.get(buffer_odd1.array());
			buffer_odd1.clear();
		}
		return targetBuffer.position();
	}

	@Benchmark
	public long mfRead_odd2(){
		while (targetBuffer.hasRemaining()) {
			int left = targetBuffer.limit() - targetBuffer.position();
			if (left < buffer_odd2.limit()){
				targetBuffer.get(new byte[left]);
				continue;
			}
			targetBuffer.get(buffer_odd2.array());
			buffer_odd1.clear();
		}
		return targetBuffer.position();
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
				.include(MappedBufferReadBenchmark.class.getSimpleName())
				.forks(1)
				.build();

		new Runner(options).run();
	}

}
