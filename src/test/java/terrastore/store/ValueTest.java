package terrastore.store;

import java.io.ByteArrayInputStream;
import org.junit.Test;
import terrastore.util.io.IOUtils;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ValueTest {

    private static final String JSON_VALUE = "{\"key\" : \"value\", "
            + "\"array\" : [\"primitive\", {\"nested\":[\"array\"]}], "
            + "\"key\" : {\"object\":\"value\"}}";

    @Test
    public void testGetBytesFromUncompressedValue() throws Exception {
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        assertArrayEquals(JSON_VALUE.getBytes("UTF-8"), value.getBytes());
    }

    @Test
    public void testGetCompressedBytesFromUncompressedValue() throws Exception {
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        assertArrayEquals(JSON_VALUE.getBytes("UTF-8"), IOUtils.readCompressed(new ByteArrayInputStream(value.getCompressedBytes())));
    }

    @Test
    public void testGetBytesFromCompressedValue() throws Exception {
        Value value = new Value(IOUtils.readAndCompress(new ByteArrayInputStream(JSON_VALUE.getBytes("UTF-8"))));
        assertArrayEquals(JSON_VALUE.getBytes("UTF-8"), value.getBytes());
    }

    @Test
    public void testGetCompressedBytesFromCompressedValue() throws Exception {
        Value value = new Value(IOUtils.readAndCompress(new ByteArrayInputStream(JSON_VALUE.getBytes("UTF-8"))));
        assertArrayEquals(JSON_VALUE.getBytes("UTF-8"), IOUtils.readCompressed(new ByteArrayInputStream(value.getCompressedBytes())));
    }

    @Test
    public void testGetInputStreamFromUncompressedValue() throws Exception {
        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        assertArrayEquals(JSON_VALUE.getBytes("UTF-8"), IOUtils.read(value.getInputStream()));
    }

    @Test
    public void testGetInputStreamFromCompressedValue() throws Exception {
        Value value = new Value(IOUtils.readAndCompress(new ByteArrayInputStream(JSON_VALUE.getBytes("UTF-8"))));
        assertArrayEquals(JSON_VALUE.getBytes("UTF-8"), IOUtils.read(value.getInputStream()));
    }
}
