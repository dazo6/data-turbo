package com.dazo66.data.turbo.util;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import static com.dazo66.data.turbo.util.Preconditions.checkNotNull;

public final class LineReader {
    private final Readable readable;
    private final Reader reader;
    private final CharBuffer cbuf = CharBuffer.allocate(0x800);
    private final char[] buf = cbuf.array();

    private final Queue<String> lines = new ArrayDeque<>();
    private final LineBuffer lineBuf = new LineBuffer() {
        @Override
        protected void handleLine(String line, String end) {
            lines.add(line);
        }
    };
    private Character split = null;

    public LineReader(Readable readable, Character split) {
        this.split = split;
        this.readable = checkNotNull(readable);
        this.reader = (readable instanceof Reader) ? (Reader) readable : null;
    }

    public LineReader(Readable readable) {
        this(readable, null);
    }

    public String readLine() throws IOException {
        while (lines.peek() == null) {
            Java8Compatibility.clear(cbuf);
            // The default implementation of Reader#read(CharBuffer) allocates a
            // temporary char[], so we call Reader#read(char[], int, int) instead.
            int read = (reader != null) ? reader.read(buf, 0, buf.length) : readable.read(cbuf);
            if (read == -1) {
                lineBuf.finish();
                break;
            }
            lineBuf.add(buf, 0, read);
        }
        return lines.poll();
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    abstract class LineBuffer {

        private StringBuilder line = new StringBuilder();
        private boolean sawReturn;

        protected void add(char[] cbuf, int off, int len) throws IOException {
            int pos = off;
            if (sawReturn && len > 0) {
                // Last call to add ended with a CR; we can handle the line now.
                if (finishLine(cbuf[pos] == '\n')) {
                    pos++;
                }
            }

            int start = pos;
            for (int end = off + len; pos < end; pos++) {
                if (split != null) {
                    if (cbuf[pos] == split) {
                        line.append(cbuf, start, pos - start);
                        finishLine(true);
                        start = pos + 1;
                    }
                } else {
                    switch (cbuf[pos]) {
                        case '\r':
                            line.append(cbuf, start, pos - start);
                            sawReturn = true;
                            if (pos + 1 < end) {
                                if (finishLine(cbuf[pos + 1] == '\n')) {
                                    pos++;
                                }
                            }
                            start = pos + 1;
                            break;

                        case '\n':
                            line.append(cbuf, start, pos - start);
                            finishLine(true);
                            start = pos + 1;
                            break;

                        default:
                            // do nothing
                    }
                }

            }
            line.append(cbuf, start, off + len - start);
        }

        private boolean finishLine(boolean sawNewline) throws IOException {
            String separator = sawReturn ? (sawNewline ? "\r\n" : "\r") : (sawNewline ? "\n" : "");
            handleLine(line.toString(), separator);
            line = new StringBuilder();
            sawReturn = false;
            return sawNewline;
        }

        protected void finish() throws IOException {
            if (sawReturn || line.length() > 0) {
                finishLine(false);
            }
        }

        protected abstract void handleLine(String line, String end) throws IOException;
    }

}
