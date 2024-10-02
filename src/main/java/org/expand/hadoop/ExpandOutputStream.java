package org.expand.hadoop;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.expand.jni.ExpandToPosix;
import org.expand.jni.ExpandFlags;

public class ExpandOutputStream extends OutputStream {

	private ExpandToPosix xpn;
	private ExpandFlags flags;
	private ExpandOutputChannel out;
	private String path;

	public ExpandOutputStream (String path, int bufsize, short replication, long blockSize, boolean append) {
		this.xpn = new ExpandToPosix();
		this.flags = xpn.jni_xpn_createFlags();
		this.path = path;
		int fd;
		if (append){
			fd = xpn.jni_xpn_open(path, flags.O_RDWR | flags.O_APPEND);
		}else {
			fd = xpn.jni_xpn_creat(path, flags.S_IRWXU | flags.S_IRWXG | flags.S_IRWXO);
		}
		this.out = new ExpandOutputChannel(fd, bufsize);
	}

	@Override
	public synchronized void close() {
		if (out == null) return;
		this.out.close();
		out = null;
	}

	@Override
	public void flush() {
		if (out == null) return;
		this.out.flush();
	}

	@Override
	public void write(byte b[], int off, int len){
		if (out == null) return;
		if (b == null) return;
		if (off < 0 || len < 0 || (off + len) > b.length) return;
		out.write(ByteBuffer.wrap(b, off, len));
	}

	@Override
	public void write(byte b[]){
		if (out == null) {
			System.out.println("BYTE ARRAY NULL");
			return;
		}
		System.out.println(new String(b));
		write(b, 0, b.length);
	}

	@Override
	public void write(int b){
		if (out == null) return;
		byte [] bytearr = {(byte) (b & 0x000000ff)};
		write(bytearr, 0, 1);
	}
}
