package org.expand.tests;

import java.nio.ByteBuffer;

import org.expand.jni.ExpandToPosix;
import org.expand.jni.Stat;
import org.expand.jni.ExpandFlags;

public class testExpandToPosix {
	
	public static void main(String[] args) {

		ExpandToPosix xpn = new ExpandToPosix();
		byte b[] = new byte[65536];
		ByteBuffer buf = ByteBuffer.allocateDirect(b.length);

		try {
			xpn.jni_xpn_init();
			int fd = xpn.jni_xpn_creat("/xpn/teststatfile", xpn.flags.O_RDWR);
			System.out.println("FD CREATED");
			int i = xpn.jni_xpn_exist("/xpn/teststatfile");
			System.out.println("DESPUES DE EXIST: " + i);
			xpn.jni_xpn_close(fd);
			
			xpn.jni_xpn_destroy();
		}catch (Exception e){
			System.out.println("EXCEPCION: " + e);
		}
	}
}
