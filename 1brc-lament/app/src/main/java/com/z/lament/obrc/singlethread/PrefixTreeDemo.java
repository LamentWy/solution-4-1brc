package com.z.lament.obrc.singlethread;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Trie Tree, also called prefix tree.
 * <br/>
 * PrefixTree with small blocks
 * */
public class PrefixTreeDemo {

	private static final String PATH ="./measurements.txt";

	//private static final String PATH ="./10mr.txt";

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
				file.seek(startPos);
				file.read(buffer);
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

	static class Trie{

		private final TrieNode root = new TrieNode();
		private static final int MAX_BYTE_VALUE = 0xFF;
		private boolean isFirstPrint = true;

		static class TrieNode {

			TrieNode[] child = new TrieNode[MAX_BYTE_VALUE];
			Data data;
			boolean isEndNode(){
				return data != null;
			}

			private static class Data {
				// data
				int min = 999;   // replace Integer.MAX_VALUE
				int max = -999;  // replace Integer.MIN_VALUE
				int sum = 0;
				int count = 0;
			}
		}

		private static int parse2Int(byte[] bytes, int tempLength, int ne) {
			int n = 0;

			if (tempLength == 2){
				n = (bytes[0] - 48) * 10 + (bytes[1] - 48);
			}else if (tempLength == 3){
				n = (bytes[0] - 48) * 100 + (bytes[1] - 48) * 10 + (bytes[2] - 48);
			}

			return n * ne;
		}

		void print() {
			byte[] nameBytes = new byte[128];
			int nameIndex = 0;

			System.out.print("{");
			recur(nameBytes,nameIndex,root.child);
			System.out.println("}");
		}

		private void recur(byte[] nameBytes, int nameIndex, TrieNode[] child) {

			for (int i = 0; i < MAX_BYTE_VALUE; i++) {
				if (child[i] != null ) {
					if (!child[i].isEndNode()){
						nameBytes[nameIndex] = (byte) i;
						recur(nameBytes, nameIndex+1, child[i].child);
					}else {
						nameBytes[nameIndex] = (byte) i;

						String name = new String(nameBytes, 0, nameIndex+1, UTF_8);

						if (isFirstPrint) {
							System.out.printf("%s=%s/%s/%s",
									name,
									child[i].data.min / 10.0,
									Math.round((double) child[i].data.sum / child[i].data.count) / 10.0,
									child[i].data.max / 10.0);
							isFirstPrint = false;
						}
						else {
							System.out.printf(", %s=%s/%s/%s",
									name,
									child[i].data.min / 10.0,
									Math.round((double) child[i].data.sum / child[i].data.count) / 10.0,
									child[i].data.max / 10.0);
						}
					}

				}
			}
		}

	}


	public static void main(String[] args) throws IOException {
		process(PATH,10000);
	}

	private static void process(String path, int blockNum) throws IOException {

		List<BlockReader> buffers = divide(path,blockNum);

		Trie resultTrie = new Trie();
		for (int i = 0; i < blockNum; i++) {
			parseTrie(buffers.get(i), resultTrie);
		}

		resultTrie.print();
	}

	private static void parseTrie(BlockReader buffer,Trie prefixTree) {

		Trie.TrieNode node;

		while (buffer.hasRemaining()) {
			node = prefixTree.root;
			// process name
			int childIndex = buffer.get();
			while (childIndex != ';'){
				childIndex = childIndex & 0xFF;

				if (node.child[childIndex] == null) { // cost 26s
					node.child[childIndex] = new Trie.TrieNode(); // store in path
				}
				node = node.child[childIndex]; // cost 53s
				childIndex = buffer.get();
			}


			byte tmp = buffer.get(); // skip ';'

			// process temperature
			int tempIndex = 0;
			int ne = 1;
			byte[] tempBytes = new byte[8];
			while (buffer.hasRemaining() && tmp != '\n') {
				if (tmp == '-') {
					ne = -1;
					// skip '-'
				}
				else if (tmp == '.') {
					// skip .
				}
				else {
					tempBytes[tempIndex] = tmp;
					tempIndex++;
				}
				tmp = buffer.get();
			}

			int tempInt = Trie.parse2Int(tempBytes,tempIndex,ne);
			updateEndNodeData(node, tempInt);
		}

	}

	private static void updateEndNodeData(Trie.TrieNode node, int tempInt) {
		if (null == node.data){
			node.data = new Trie.TrieNode.Data();
		}

		if (node.data.min > tempInt){
			node.data.min = tempInt;
		}
		if (node.data.max < tempInt){
			node.data.max = tempInt;
		}
		node.data.sum = node.data.sum + tempInt;
		node.data.count++;
	}

	private static List<BlockReader> divide(String path, int blockNum) throws IOException {

		// create file
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

}
