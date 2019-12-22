package com.cw.bigdata.hbase.format;

import com.cw.bigdata.hbase.bean.CacheData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MysqlOutputFormat extends OutputFormat<Text, CacheData> {

    class MysqlRecordWriter extends RecordWriter<Text, CacheData> {

        private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
        private static final String MYSQL_URL = "jdbc:mysql://192.168.139.101:3306/company?useUnicode=true&characterEncoding=UTF-8";
        private static final String MYSQL_USERNAME = "root";
        private static final String MYSQL_PASSWORD = "123456";

        private Connection connection;
        private Configuration conf;

        public MysqlRecordWriter(Configuration configuration) {
            conf = configuration;
            try {
                Class.forName(MYSQL_DRIVER_CLASS);
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void write(Text key, CacheData data) throws IOException, InterruptedException {

            String mysqlTable = conf.get("--mysql-table");

            StringBuilder columns = new StringBuilder();
            StringBuilder columnParams = new StringBuilder();
            List<String> paramVals = new ArrayList<String>();

            Iterator<String> iterator = data.getDataMap().keySet().iterator();
            while (iterator.hasNext()) {
                String colName = iterator.next();
                columns.append(colName).append(",");
                columnParams.append("?").append(",");
                paramVals.add(data.getDataMap().get(colName));
            }

            String colString = columns.toString().substring(0, columns.length() - 1);
            String colParamString = columnParams.toString().substring(0, columnParams.length() - 1);

            String sql = "insert into " + mysqlTable + " (" + colString + ") values(" + colParamString + ")";
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.prepareStatement(sql);
                for (int i = 0; i < paramVals.size(); i++) {
                    preparedStatement.setObject(i + 1, paramVals.get(i));
                }
                //preparedStatement.setObject(2, data.getCount());
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public RecordWriter<Text, CacheData> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        return new MysqlRecordWriter(configuration);
    }

    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }


    private FileOutputCommitter committer = null;

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

}
