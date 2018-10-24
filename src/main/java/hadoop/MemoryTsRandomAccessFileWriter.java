package hadoop;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class MemoryTsRandomAccessFileWriter implements ITsRandomAccessFileWriter {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    @Override
    public long getPos() throws IOException {
        return outputStream.size();
    }

    @Override
    public void truncate(long length) throws IOException {
        throw new IOException("not implmenent the `truncate` function");

    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);

    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public OutputStream getOutputStream() {
        return outputStream;
    }
}
