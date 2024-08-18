package org.example.benchmark.simd;

/**
 * double it with super word
 * <br/>
 *
 * jvm args:
 * <br/>
 * -XX:CompileCommand=print,*SuperWordDemo.doubleIt
 * <br/>
 * -XX:CompileCommand=compileonly,*SuperWordDemo.doubleIt
 * <br/>
 * shutdown SuperWord:
 * <br/>
 * 	-XX:-UseSuperWord
 *</p>
 * */
public class SuperWordDemo {

	static int N = 117;

	public static void main(String[] args) {
		int[] intArray = new int[N];

		for (int i = 0; i < 10000; i++) {
			init(intArray);
			doubleIt(intArray);
		}

	}

	private static void doubleIt(int[] intArray) {
		for (int i = 0; i < N; i++) {
			intArray[i] = intArray[i] * 2;   // <- this line will apply super word
		}
	}

	private static void init(int[] intArray) {
		for (int i = 0; i < N; i++) {
			intArray[i] = i;
		}
	}
}
