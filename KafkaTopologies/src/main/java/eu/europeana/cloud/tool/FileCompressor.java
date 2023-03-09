package eu.europeana.cloud.tool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class FileCompressor {

    private static final boolean ON = false;
    public byte[] decompress(byte[] input)
            throws IOException, DataFormatException {
        if (!ON) {
            return input;
        }

        // Create an Inflater object to compress the data
        Inflater decompressor = new Inflater(false);

        // Set the input for the decompressor
        decompressor.setInput(input);

        // Decompress data
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        byte[] readBuffer = new byte[1024];
        int readCount = 0;

        while (!decompressor.finished()) {
            readCount = decompressor.inflate(readBuffer);
            if (readCount > 0) {
                // Write the data to the output stream
                bao.write(readBuffer, 0, readCount);
            }
        }

        // End the decompressor
        decompressor.end();

        // Return the written bytes from the output stream
        return bao.toByteArray();
    }


    public byte[] compress(byte[] input){
        if (!ON) {
            return input;
        }
        Deflater compressor = new Deflater(Deflater.BEST_COMPRESSION,false);
        compressor.setInput(input);
        compressor.finish();

        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        byte[] readBuffer = new byte[1024];
        int readCount = 0;

        while (!compressor.finished()) {
            readCount = compressor.deflate(readBuffer);
            if (readCount > 0) {
                // Write compressed data to the output stream
                bao.write(readBuffer, 0, readCount);
            }
        }

        // End the compressor
        compressor.end();

        // Return the written bytes from output stream
        String compressedFile = new String(bao.toByteArray());
        System.out.println(compressedFile);
        System.out.println(compressedFile.length());
        return bao.toByteArray();
    }
}
