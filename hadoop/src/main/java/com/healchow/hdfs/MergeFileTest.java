package com.healchow.hdfs;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

/**
 * 合并小文件
 *
 * @author healchow
 * @since 2021/06/20 17:21
 */
public class MergeFileTest {

    @Test
    public void testMergeFile() throws Exception {
        // 获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop:9000"), new Configuration(), "healchow");
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/upload/merged_by_java.txt"));
        // 获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        // 通过本地文件系统获取文件列表，这里必须指定路径
        FileStatus[] fileStatuses = local.listStatus(new Path("file:/Users/healchow/bigdata/test"));
        for (FileStatus fileStatus : fileStatuses) {
            // 创建输入流，操作完即关闭
            if (fileStatus.getPath().getName().contains("user")) {
                FSDataInputStream inputStream = local.open(fileStatus.getPath());
                IOUtils.copy(inputStream, outputStream);
                IOUtils.closeQuietly(inputStream);
            }
        }

        // 关闭输出流和文件系统
        IOUtils.closeQuietly(outputStream);
        local.close();
        fileSystem.close();
    }

}
