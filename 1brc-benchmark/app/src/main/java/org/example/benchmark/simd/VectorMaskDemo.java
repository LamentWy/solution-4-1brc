package org.example.benchmark.simd;

import java.util.Arrays;
import java.util.Random;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.runner.RunnerException;


public class VectorMaskDemo {

	private static final int Multiple_Of_VLength = 8*2;
	// 任意非对齐的长度
	private static final int UnMatch_SIZE = 8*2+1;

	private static final VectorSpecies<Integer> IntSpecies = IntVector.SPECIES_PREFERRED;

	private int[] a_multi_vlength = new int[Multiple_Of_VLength];
	private int[] b_multi_vlength = new int[Multiple_Of_VLength];

	private int[] a = new int[UnMatch_SIZE];
	private int[] b = new int[UnMatch_SIZE];

	IntVector a_vector;
	IntVector b_vector;
	IntVector c_vector;
	IntVector d_vector;

	public void setup(){
		Random random = new Random();
		for (int i = 0; i < a_multi_vlength.length; i++) {
			a_multi_vlength[i] = random.nextInt(0,100);
			b_multi_vlength[i] = random.nextInt(100,200);
		}

		for (int i = 0; i < a.length; i++) {
			a[i] = random.nextInt(0,100);
			b[i] = random.nextInt(100,200);
		}
	}


	public int[] add_multiple(){
		int[] result = new int[a_multi_vlength.length];

		for (int i = 0; i < IntSpecies.loopBound(a_multi_vlength.length); i+=IntSpecies.length()) {
			a_vector = IntVector.fromArray(IntSpecies, a_multi_vlength, i);
			b_vector = IntVector.fromArray(IntSpecies, b_multi_vlength, i);
			IntVector mid_r_v = a_vector.add(b_vector);
			mid_r_v.intoArray(result,i);
		}
		return result;
	}


	public int[] add_use_mask(){
		int[] result = new int[a.length];
		for (int i = 0; i < a.length; i+=IntSpecies.length()) {
			// mask
			VectorMask<Integer> mask = IntSpecies.indexInRange(i, a.length);

			c_vector = IntVector.fromArray(IntSpecies, a, i,mask);
			d_vector = IntVector.fromArray(IntSpecies, b, i,mask);
			IntVector mid_r_v = c_vector.add(d_vector);
			mid_r_v.intoArray(result,i,mask);
		}

		// no tail process

		return result;
	}

	public int[] add_no_mask(){
		int[] result = new int[a.length];
		for (int i = 0; i < IntSpecies.loopBound(a.length); i+=IntSpecies.length()) {


			c_vector = IntVector.fromArray(IntSpecies, a, i);
			d_vector = IntVector.fromArray(IntSpecies, b, i);
			IntVector mid_r_v = c_vector.add(d_vector);
			mid_r_v.intoArray(result,i);
		}

		// need tail process

		// in almost all cases, IntSpecies.length() always be 2^n
		int tailSize = a.length & (IntSpecies.length() - 1); // aka: a.length % IntSpecies.length();

		for (int i = a.length - tailSize; i < a.length; i++) {
			result[i] = a[i] + b[i];
		}

		return result;
	}

	public int[] add_mask_and_tail(){
		int[] result = new int[a.length];
		for (int i = 0; i < IntSpecies.loopBound(a.length); i+=IntSpecies.length()) {
			// mask
			VectorMask<Integer> mask = IntSpecies.indexInRange(i, a.length);

			c_vector = IntVector.fromArray(IntSpecies, a, i,mask);
			d_vector = IntVector.fromArray(IntSpecies, b, i,mask);
			IntVector mid_r_v = c_vector.add(d_vector);
			mid_r_v.intoArray(result,i,mask);
		}

		int tailSize = a.length & (IntSpecies.length() - 1);

		for (int i = a.length - tailSize; i < a.length; i++) {
			result[i] = a[i] + b[i];
		}

		return result;
	}

	private static void printResult(String desc,int[] result) {
		System.out.println("--- " + desc + " ---");
		System.out.println("array size : " + result.length);
		Arrays.stream(result).forEach(i -> System.out.print(i + " "));
		System.out.println();
	}

	public static void main(String[] args) throws RunnerException {

		VectorMaskDemo vectorMaskDemo = new VectorMaskDemo();
		vectorMaskDemo.setup();
		int[] result_1 = vectorMaskDemo.add_multiple();
		int[] result_2 = vectorMaskDemo.add_use_mask();
		int[] result_3 = vectorMaskDemo.add_no_mask();
		int[] result_4 = vectorMaskDemo.add_mask_and_tail();

		printResult("VLength 倍数版", result_1);
		printResult("use mask", result_2);
		printResult("no mask", result_3);
		printResult("mask and tail",result_4);
	}


}
