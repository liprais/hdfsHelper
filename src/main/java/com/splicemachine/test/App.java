package com.splicemachine.test;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello World!");
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        String coreSiteXml = "/Users/liuxiao/spliceengine/platform_it/target/classes/core-site.xml";
        String KRB5_CONF_PATH = "/Users/liuxiao/spliceengine/platform_it/target/krb5.conf";
        conf.addResource(new Path(coreSiteXml));
        System.setProperty("java.security.krb5.conf", KRB5_CONF_PATH);

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hdfs/example.com@EXAMPLE.COM", "/Users/liuxiao/spliceengine/platform_it/target/splice.keytab");
            System.out.println("I am in!");
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (FileSystem fs = FileSystem.get(conf)) {

            Path localDataPath = new Path("file:///Users/liuxiao/Downloads/basic");
            Path targetPath = new Path("/data");
            Path badPath = new Path("/BAD");

            fs.delete(targetPath,Boolean.TRUE);
            fs.delete(badPath,Boolean.TRUE);

            fs.mkdirs(targetPath);
            fs.mkdirs(badPath);

            fs.copyFromLocalFile(localDataPath, targetPath);

            fs.setOwner(badPath, "hbase", "hbase");
            fs.setOwner(targetPath, "hbase", "hbase");
            Path dfsDataPath = new Path(targetPath, localDataPath.getName());
            fs.setOwner(dfsDataPath, "hbase", "hbase");
            setOwner(fs,"hbase","hbase",dfsDataPath.toString());
            checkStatus(fs,dfsDataPath);
            checkStatus(fs,badPath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void setOwner(FileSystem fs,String owner,String group,String path) throws IOException {
        Path targetPath = new Path(path);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(targetPath, Boolean.TRUE);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            fs.setOwner(fileStatus.getPath(),owner,group);
        }
    }

    private static void checkStatus(FileSystem fs,Path path) throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(path, Boolean.TRUE);
        while (locatedFileStatusRemoteIterator.hasNext())
        {
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            String Path = locatedFileStatus.getPath().toString();
            String Owner = locatedFileStatus.getOwner();
            String Group = locatedFileStatus.getGroup();
            System.out.println(Path + ":" + Owner + ":" + Group);
        }
    }
}