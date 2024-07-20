package org.example.createfile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

public class CreateFile {

	public static void main(String[] args) throws IOException {
		int fileSize_1GB = 1024 * 1024 * 1024;
		String file_1GB = "./files/targetFile-1GB.txt";
		// 1mb, 128mb, 512mb
		int fileSize_1MB = 1024 * 1024;
		String file_1MB = "./files/targetFile-1MB.txt";
		int fileSize_128MB = 128*1024*1024;
		String file_128MB = "./files/targetFile-128MB.txt";
		int fileSize_512MB = 512*1024*1024;
		String file_512MB = "./files/targetFile-512MB.txt";


		createFile(fileSize_1MB,file_1MB);
		createFile(fileSize_128MB,file_128MB);
		createFile(fileSize_512MB,file_512MB);
		createFile(fileSize_1GB,file_1GB);
	}

	private static void createFile(int fileSize,String fileName) throws IOException {

		int buffersize = 1024*1024;
		ByteBuffer buffer = ByteBuffer.allocate(buffersize);

		while(buffer.hasRemaining()) {
			buffer.put((byte)new Random().nextInt(48,57));
		}

		Path path = null;
		try {
			path = Files.createFile(Path.of(fileName));
		}
		catch (IOException e) {
			System.out.println(fileName + " Created already!");
			System.exit(0);
		}

		try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) {
			file.setLength(fileSize);
			int limit = fileSize / buffersize;
			for (int i = 0; i < limit; i++) {
				file.write(buffer.array(), 0, buffersize);
			}
		}
	}
}
