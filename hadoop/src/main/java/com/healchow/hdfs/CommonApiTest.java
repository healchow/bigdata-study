package com.healchow.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * 练习 HDFS 的常用 API
 *
 * @author healchow
 * @since 2021/06/09 11:50
 */
public class CommonApiTest {
    
    private static final String HDFS_SERVER_URI = "hdfs://hadoop:9000";

    /**
     * 获取 FileSystem - FileSystem.get()
     */
    @Test
    public void testGetFileSystem1() throws IOException {
        // 创建 Configuration 对象
        Configuration conf = new Configuration();

        // 指定文件系统类型
        conf.set("fs.defaultFS", HDFS_SERVER_URI);

        // 获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(conf);
        // FileSystem fileSystem = FileSystem.get(new URI(HDFS_SERVER_URI), new Configuration());

        // 结果：DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_1219793882_1, ugi=healchow (auth:SIMPLE)]]
        System.out.println(fileSystem);

        // 关闭文件系统
        fileSystem.close();
    }

    /**
     * 获取 FileSystem - FileSystem.newInstance()
     */
    @Test
    public void testGetFileSystem2() throws IOException {
        // 创建 Configuration 对象
        Configuration conf = new Configuration();

        // 指定文件系统类型
        conf.set("fs.defaultFS", HDFS_SERVER_URI);

        // 获取指定的文件系统
        FileSystem fileSystem = FileSystem.newInstance(conf);
        // FileSystem fileSystem = FileSystem.newInstance(new URI(HDFS_SERVER_URI), new Configuration());

        System.out.println(fileSystem);
        fileSystem.close();
    }

    /**
     * 通过 HDFS URL 创建目录、写入文件
     */
    @Test
    public void testPutFile() throws IOException, URISyntaxException {
        // 创建测试目录（可创建多级目录）
        FileSystem fileSystem = FileSystem.newInstance(new URI(HDFS_SERVER_URI), new Configuration());
        boolean result = fileSystem.mkdirs(new Path("/test/input"));
        System.out.println("mkdir result: " + result);

        // 创建文件，若存在则覆盖，返回的是写入文件的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/input/hello.txt"), true);
        String content = "hello,hadoop\nhello,hdfs";
        outputStream.write(content.getBytes(StandardCharsets.UTF_8));

        // 关闭流（不抛出异常）
        IOUtils.closeQuietly(outputStream);
    }

    /**
     * 向 HDFS 上传文件 - copyFromLocalFile()
     */
    @Test
    public void testUploadFile() throws URISyntaxException, IOException {
        // 获取 FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_SERVER_URI), new Configuration());

        // 从本地上传文件，两个参数都要指定到具体的文件
        fileSystem.copyFromLocalFile(new Path("/Users/healchow/bigdata/core-site.xml"),
                new Path("/test/upload/core-site.xml"));

        // 关闭FileSystem
        fileSystem.close();
    }

    /**
     * 通过 HDFS URL 获取文件并下载 - IOUtils.copy() 方法
     */
    @Test
    public void testDownFileByUrl() throws IOException {
        // 注册 HDFS URL
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        // 获取 HDFS 文件的输入流
        InputStream inputStream = new URL("hdfs://hadoop:9000/test/input/hello.txt").openStream();
        // 获取本地文件的输出流（绝对路径，文件夹必须存在）
        FileOutputStream outputStream = new FileOutputStream("/Users/healchow/bigdata/test/hello.txt");

        // 拷贝文件
        IOUtils.copy(inputStream, outputStream);

        // 关闭流（不抛出异常）
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }


    /**
     * 通过 FileSystem 获取文件并下载 - IOUtils.copy() 方法
     */
    @Test
    public void testDownloadFile() throws URISyntaxException, IOException {
        // 获取 FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_SERVER_URI), new Configuration());

        // 获取 HDFS 文件的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/test/input/hello.txt"));

        // 获取本地文件的输出流
        FileOutputStream outputStream = new FileOutputStream("/Users/healchow/bigdata/test/hello1.txt");

        // 拷贝文件
        IOUtils.copy(inputStream, outputStream);

        // 关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    /**
     * 通过 FileSystem 获取文件并下载 - copyToLocalFile() 方法
     */
    @Test
    public void testDownloadFileByCopyTo() throws URISyntaxException, IOException, InterruptedException {
        // 获取 FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_SERVER_URI), new Configuration(), "root");

        // copyToLocalFile 拷贝文件到本地，会下载 CRC 校验文件
        fileSystem.copyToLocalFile(new Path("/test/input/hello.txt"),
                new Path("/Users/healchow/bigdata/test/hello2.txt"));

        // 关闭 FileSystem
        fileSystem.close();
    }

    /**
     * 遍历 HDFS 文件
     */
    @Test
    public void testListFiles() throws URISyntaxException, IOException {
        // 获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_SERVER_URI), new Configuration());

        // 递归获取 /test 目录下所有的文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/test"), true);

        // 遍历文件
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();

            // 获取文件的绝对路径：hdfs://hadoop:9000/xxx
            System.out.println("filePath: " + fileStatus.getPath());

            // 文件的 block 信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("blockHost: " + host);
                }
            }
            System.out.println("blockSize: " + blockLocations.length);
        }
    }

    /**
     * 合并小文件
     */
    @Test
    public void testMergeFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取 FileSystem
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_SERVER_URI), new Configuration(), "root");

        // 获取要合并的大文件的输出流（用来写文件）
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/merge_file.txt"));

        // 获取本地文件系统，通过它来获取本地某个目录下的所有文件
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("/Users/healchow/bigdata/test"));

        // 遍历获取每个本地文件的输入流（用来读文件）
        for (FileStatus fileStatus : fileStatuses) {
            // 忽略以 "." 开头的隐藏文件（CRC 校验文件会导致乱码）
            if (fileStatus.getPath().getName().startsWith(".")) {
                continue;
            }
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());

            // 将小文件的数据复制到大文件
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }

        // 关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }

    /**
     * 通过下载文件，测试访问权限控制
     */
    @Test
    public void testAccessControl() throws Exception {
        // 开启权限控制后，当前用户（启动 NameNode 的用户）应当能成功访问
        // FileSystem fileSystem = FileSystem.get(new URI(HDFS_SERVER_URI), new Configuration());
        // 伪造其他用户访问，应当访问失败
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_SERVER_URI), new Configuration(), "testuser");

        fileSystem.copyToLocalFile(new Path("/test/config/core-site.xml"),
                new Path("file:/Users/healchow/bigdata/core-site.xml"));

        fileSystem.close();
    }

}
