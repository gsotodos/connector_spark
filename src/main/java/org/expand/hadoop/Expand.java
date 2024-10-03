package org.expand.hadoop;

import java.net.URI;
import java.net.InetAddress;
import java.io.IOException;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.util.Progressable;

import org.expand.jni.ExpandToPosix;
import org.expand.jni.ExpandFlags;
import org.expand.jni.Stat;

public class Expand extends FileSystem {

	private ExpandToPosix xpn;
	private URI uri;
	private Path workingDirectory;
	public ExpandFlags flags;
	private long blksize;
	private int bufsize;
	private int xpn_replication = 1;
	private boolean initialized;

	public Expand(){
		this.xpn = new ExpandToPosix();
		this.uri = URI.create("xpn:///");
		this.setWorkingDirectory(new Path("/xpn"));
		this.flags = this.xpn.flags;
		this.initialized = false;
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress){
		String f_str = f.toString();
		if (f_str.startsWith("xpn:")) f_str = f_str.substring(4);

		if (this.xpn.jni_xpn_exist(f_str) == 0) xpn.jni_xpn_creat(f_str, flags.S_IRWXU | flags.S_IRWXG | flags.S_IRWXO);

		return new FSDataOutputStream(new ExpandOutputStream(f_str, bufsize, (short) 0, blksize, true), statistics);
	}

	public void close() throws IOException {

	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
		String f_str = f.toString();
		if (f_str.startsWith("xpn:")) f_str = f_str.substring(4);
		Path parent = f.getParent();

		if (this.xpn.jni_xpn_exist(f_str) == 0) {
			if (overwrite) delete(f, false);
			else throw new IOException("File already exists: " + f_str);
		}else{
			if (this.xpn.jni_xpn_exist(parent.toString()) != 0) mkdirs(parent, FsPermission.getFileDefault());
		}

		return new FSDataOutputStream(new ExpandOutputStream(f_str, bufsize, replication, 
					blksize, false), statistics);
	}

	@Override
	public boolean delete(Path path, boolean recursive){
		String path_str = path.toString();
		if (path_str.startsWith("xpn:")) path_str = path_str.substring(4);
		
		if (this.xpn.jni_xpn_exist(path_str) != 0) return false;
		if (!isDirectory(path)) return this.xpn.jni_xpn_unlink(path_str) == 0;
		if (!recursive) return this.xpn.jni_xpn_rmdir(path_str) == 0;

		String [] str = this.xpn.jni_xpn_getDirContent(path_str);
		String deletePath;
		boolean res;

		for (int i = 0; i < str.length; i++){
			if (str[i].equals(".") || str[i].equals("..")) continue;
			res = delete(new Path(path_str + "/" + str[i]), true);
			if (!res) return false;
		}

		return this.xpn.jni_xpn_rmdir(path_str) == 0;
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file,
      								long start, long len) throws IOException {
		String path = file.getPath().toString();
		if (path.startsWith("xpn:")) path = path.substring(4);

		if (file == null) {
			throw new IOException("File does not exist: " + path);
		}

		if (start < 0 || len < 0) {
			throw new IllegalArgumentException("Invalid start or len parameter");
		}

		if (getLength(file.getPath()) <= start) {
			return new BlockLocation[0];
		}

		int splits = (int) Math.ceil(len / blksize) + 1;

		BlockLocation[] blkloc = new BlockLocation[splits];

		String[][] url_v = new String[splits][xpn_replication]; 
		String[][] host = new String[splits][xpn_replication];

		for (int i = 0; i < splits; i++){
			int res = this.xpn.jni_xpn_get_block_locality(path, i * blksize, url_v[i]);

			for (int j = 0; j < xpn_replication; j++) {
				InetAddress addr = InetAddress.getByName(url_v[i][j]);
				host[i][j] = addr.getHostAddress();
			}

			long split_len = blksize;
			if (i == splits - 1){
				split_len = len - (blksize * i);
			}

			blkloc[i] = new BlockLocation(url_v[i], host[i], i * blksize, split_len);
	}

	return blkloc;
  }

	@Override
	public FileStatus getFileStatus (Path path) throws IOException {
		String path_str = path.toString();
		if (path_str.startsWith("xpn:")) path_str = path_str.substring(4);

		if (this.xpn.jni_xpn_exist(path_str) != 0) {
			throw new IOException("File does not exist: " + path_str);
		};

		Stat stats = this.xpn.jni_xpn_stat(path_str);
		boolean isdir = this.xpn.jni_xpn_isDir(stats.st_mode) != 0;
		String username = this.xpn.jni_xpn_getUsername((int) stats.st_uid);
		String groupname = this.xpn.jni_xpn_getGroupname((int) stats.st_gid);
		FsPermission permission = new FsPermission(Integer.toOctalString(stats.st_mode & 0777));

		return new FileStatus(stats.st_size, isdir, 0, stats.st_blksize,
					stats.st_mtime * 1000, stats.st_atime * 1000, 
					permission, username, groupname, path);
	}

	public long getLength (Path path) throws IOException {
		String path_str = path.toString();
		if (path_str.startsWith("xpn:")) path_str = path_str.substring(4);

		if (this.xpn.jni_xpn_exist(path_str) != 0) {
			throw new IOException("File does not exist: " + path_str);
		};

		return this.xpn.jni_xpn_stat(path_str).st_size;
	}

	@Override
	public URI getUri() {
		return this.uri;
	}

	@Override
	public Path getWorkingDirectory() {
		return this.workingDirectory;
	}

	public void initialize(URI uri, Configuration conf) throws IOException {
		try{
			super.initialize(getUri(), conf);
			this.xpn.jni_xpn_init();
			this.blksize = conf.getLong("xpn.block.size", 134217728);
    		this.bufsize = conf.getInt("xpn.file.buffer.size", 134217728);
    		this.xpn_replication = conf.getInt("xpn.block.replication", 1);
			this.initialized = true;
		}catch(Exception e){
			System.out.println("Excepcion en INITIALIZE: " + e);
			return;
		}
	}
	
	@Override
	public boolean isDirectory (Path path) {
		String path_str = path.toString();
		if (path_str.startsWith("xpn:")) path_str = path_str.substring(4);
		try {
			Stat stats = this.xpn.jni_xpn_stat(path_str);
			return this.xpn.jni_xpn_isDir(stats.st_mode) != 0;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		String f_str = f.toString();
		if (f_str.startsWith("xpn:")) f_str = f_str.substring(4);
		if (this.xpn.jni_xpn_exist(f_str) != 0)
			throw new IOException("File does not exist: " + f_str);

		if (!isDirectory(f)){
			FileStatus [] list = new FileStatus[1];
			list[0] = getFileStatus(f);
			return list;
		}

		String str [] = this.xpn.jni_xpn_getDirContent(f_str);
		FileStatus list [] = new FileStatus [str.length];
		for (int i = 0; i < list.length; i++){
			if (str.equals(".") || str.equals("..")) continue;
			list[i] = getFileStatus(new Path(f_str + "/" + str[i]));
		}

		return list;
	}

	private Path makeAbsolute (Path path) {
		String fullPath = this.workingDirectory.toString() + path.toString();

		return new Path (fullPath);
	}

	@Override
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		String path_str = path.toString();
		if (path_str.startsWith("xpn:")) path_str = path_str.substring(4);
		String relPath = "/xpn";
		String absPath;
		String [] dirs = path_str.split("/");

		for (int i = 1; i < dirs.length; i++){
			if (dirs[i].equals("xpn") && i == 1) continue;
			relPath += "/" + dirs[i];
			if (this.xpn.jni_xpn_exist(relPath) == 0) continue;
			int res = this.xpn.jni_xpn_mkdir(relPath , permission.toShort());
			if (res != 0) throw new IOException("Directory could not be created: " + relPath);
		}

		return true;
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize){
		String f_str = f.toString();
		if (f_str.startsWith("xpn:")) f = new Path (f_str.substring(4));

		return new FSDataInputStream(new ExpandFSInputStream(f_str, bufsize, statistics));
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		String src_str = src.toString();
		String dst_str = dst.toString();
		if (src_str.startsWith("xpn:")) src = new Path (src_str.substring(4));
		if (dst_str.startsWith("xpn:")) dst = new Path (dst_str.substring(4));

		if (this.xpn.jni_xpn_exist(src_str) != 0) throw new IOException("File does not exist: " + src_str);
		if (this.xpn.jni_xpn_exist(dst_str) == 0) throw new IOException("File already exists: " + dst_str);

		int res = xpn.jni_xpn_rename(src_str, dst_str);

		return res == 0;
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		if (new_dir.toString().startsWith("xpn:")) new_dir = new Path (new_dir.toString().substring(4));
		this.workingDirectory = new_dir;
	}

	@Override
	public void setPermission(Path path, FsPermission perm) throws IOException {

	}
}
