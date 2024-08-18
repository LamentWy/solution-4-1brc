package org.example.benchmark.simd;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
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


/**
 *  double it
 * */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SIMDV2Benchmark {

	private static final int SIZE = (2 << 14) + 1;
	private static final int LoopBound = 10_000;

	private static final VectorSpecies<Integer> IntSpecies = IntVector.SPECIES_PREFERRED;
	private static final Random RandomINT = new Random();

	private int[] a ;
	private int[] b ;
	private int[] c ;
	private int[] d ;

	@Setup(Level.Invocation)
	public void setup(){
		a = new int[SIZE];
		b = new int[SIZE];
		c = new int[SIZE];
		d =	new int[SIZE];

		for (int i = 0; i < SIZE; i++) {
			a[i] = RandomINT.nextInt(0,100);
			b[i] = RandomINT.nextInt(0,100);
			c[i] = RandomINT.nextInt(0,100);
			d[i] = RandomINT.nextInt(0,100);
		}
	}

	@TearDown(Level.Invocation)
	public void tearDown(){
		a = null;
		b = null;
		c = null;
		d =	null;
	}

	@Benchmark
	public void double_it_Scalar(){
		for (int i = 0; i < LoopBound; i++) {
			sink(double_it());
		}
	}

	private int[] double_it() {
		for (int i = 0; i < SIZE; i++){
			a[i] = a[i] * 2;
		}
		return a;
	}

	@Benchmark
	public void double_it_Vector(){
		for (int i = 0; i < LoopBound; i++) {
			sink(double_use_mask());
		}

	}

	public int[] double_use_mask(){
		for (int i = 0; i < SIZE; i += IntSpecies.length()) {
			// mask
			VectorMask<Integer> mask = IntSpecies.indexInRange(i,SIZE);

			IntVector intVector = IntVector.fromArray(IntSpecies, b, i, mask);
			intVector.mul(2).intoArray(b,i,mask);
		}
		// no tail process
		return b;
	}

	@Benchmark
	public void double_it_Vector_Tail(){
		for (int i = 0; i < LoopBound; i++) {
			sink(double_tail());
		}

	}

	private int[] double_tail() {
		for (int i = 0; i < IntSpecies.loopBound(SIZE); i+= IntSpecies.length()) {
			// mask
			VectorMask<Integer> mask = IntSpecies.indexInRange(i,SIZE);

			IntVector intVector = IntVector.fromArray(IntSpecies, c, i, mask);
			intVector.mul(2).intoArray(c,i,mask);
		}
		// tail process
		int tailSize = SIZE & (IntSpecies.length() - 1);
		for (int i = SIZE - tailSize; i < SIZE; i++) {
			c[i] = c[i] * 2;
		}
		return c;
	}

	@Benchmark
	public void double_it_Vector_no_mask_tail(){
		for (int i = 0; i < LoopBound; i++) {
			sink(double_no_mask());
		}
	}

	private int[] double_no_mask() {
		for (int i = 0; i < IntSpecies.loopBound(SIZE); i+= IntSpecies.length()) {
			IntVector intVector = IntVector.fromArray(IntSpecies, d, i);
			intVector.mul(2).intoArray(d,i);
		}
		// tail process
		int tailSize = SIZE & (IntSpecies.length() - 1);
		for (int i = SIZE - tailSize; i < SIZE; i++) {
			d[i] = d[i] * 2;
		}
		return d;
	}

	@CompilerControl(CompilerControl.Mode.DONT_INLINE)
	public static void sink(int[] x) {
		// IT IS VERY IMPORTANT TO MATCH THE SIGNATURE TO AVOID AUTOBOXING.
		// The method intentionally does nothing.
	}


	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(SIMDV2Benchmark.class.getSimpleName())
				.forks(1)
				.jvmArgs("-XX:-UseSuperWord","--add-modules=jdk.incubator.vector")
				.build();
		new Runner(opt).run();
	}
}
