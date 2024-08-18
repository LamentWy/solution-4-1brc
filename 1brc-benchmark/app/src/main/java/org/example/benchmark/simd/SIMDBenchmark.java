package org.example.benchmark.simd;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 *  simple simd demo : add two arrays
 * */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 0)
@Measurement(iterations = 20)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SIMDBenchmark {

	private static final int SIZE = 8; // SIZE = shape/32 , 256/32 = 8
	private static final VectorSpecies<Integer> Int_Vector_Species = IntVector.SPECIES_PREFERRED;

	private int[] a = new int[SIZE];
	private int[] b = new int[SIZE];
	private int[] result = new int[SIZE];

	IntVector aVector;
	IntVector bVector;


	@Setup
	public void setup(){
		Random random = new Random();
		for (int i = 0; i < SIZE; i++) {
			a[i] = random.nextInt(0,100);
			b[i] = random.nextInt(100,200);
		}

		aVector = IntVector.fromArray(Int_Vector_Species, a, 0);
		bVector = IntVector.fromArray(Int_Vector_Species, b, 0);
	}

	@Benchmark
	public int[] add_Scalar(){
		for (int i = 0; i < SIZE; i++){
			result[i] = a[i] + b[i];
		}
		return result;
	}


	@Benchmark
	public int[] add_Vector(Blackhole bh){
		return aVector.add(bVector).toArray();
	}


	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(SIMDBenchmark.class.getSimpleName())
				.forks(1)
				.jvmArgs("-XX:-UseSuperWord","--add-modules=jdk.incubator.vector")
				.build();
		new Runner(opt).run();
	}
}
