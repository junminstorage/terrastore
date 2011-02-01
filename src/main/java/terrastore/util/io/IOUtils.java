/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.util.io;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Sergio Bossa
 */
public class IOUtils {

    public static byte[] read(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        read(input, output);
        return output.toByteArray();
    }

    public static byte[] readCompressed(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        readCompressed(input, output);
        return output.toByteArray();
    }

    public static byte[] readAndCompress(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        readAndCompress(input, output);
        return output.toByteArray();
    }

    public static void read(InputStream input, OutputStream output) throws IOException {
        byte[] read = ThreadLocalByteBuffer.getBuffer();
        int nr = input.read(read);
        while (nr != -1) {
            output.write(read, 0, nr);
            nr = input.read(read);
        }
    }

    public static void readCompressed(InputStream input, OutputStream output) throws IOException {
        LZFInputStream decoder = null;
        try {
            decoder = new LZFInputStream(input);
            byte[] read = ThreadLocalByteBuffer.getBuffer();
            int nr = decoder.read(read);
            while (nr != -1) {
                output.write(read, 0, nr);
                nr = decoder.read(read);
            }
        } finally {
            if (decoder != null) {
                decoder.close();
            }
        }
    }

    public static void readAndCompress(InputStream input, OutputStream output) throws IOException {
        LZFOutputStream encoder = null;
        boolean success = true;
        try {
            encoder = new LZFOutputStream(output);
            //
            byte[] read = ThreadLocalByteBuffer.getBuffer();
            int nr = input.read(read);
            while (nr != -1) {
                encoder.write(read, 0, nr);
                nr = input.read(read);
            }
            encoder.close();
        } catch (IOException ex) {
            success = false;
            throw new IllegalStateException(ex.getMessage(), ex);
        } finally {
            if (!success && encoder != null) {
                encoder.close();
            }
        }
    }

    public static boolean isCompressed(byte[] data) {
        return data[0] == 'Z' && data[1] == 'V';
    }

    public static boolean isCompressed(InputStream stream) {
        try {
            boolean available = stream.available() >= 2;
            boolean markSupported = stream.markSupported();
            if (available && markSupported) {
                stream.mark(2);
                boolean compressed = stream.read() == 'Z' && stream.read() == 'V';
                stream.reset();
                return compressed;
            } else if (!available) {
                throw new IllegalStateException("Not enough bytes to determine stream compression!");
            } else {
                throw new IllegalStateException("Unable to determine stream compression!");
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    public static InputStream getCompressedInputStream(byte[] bytes) throws IOException {
        return new LZFInputStream(new ByteArrayInputStream(bytes));
    }

}
