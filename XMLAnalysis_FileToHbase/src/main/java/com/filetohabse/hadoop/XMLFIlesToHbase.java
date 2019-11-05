package com.filetohabse.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//编写自定义mr程序，在从hdfs中获取到每一个xml文件，将文件作为mapper的输入，对在mapper中对每一个xml文件进行解析
//并把解析结果提交的Reduce阶段，Reduce阶段负责将解析出来的结果传至hbase数据库中、
//程序运行先决条件：已创建好Hbase表，或将建立Hbase表的步骤放至前一步文件上传HDFS程序
//hbase论文数据表结构
//以论文文章名为rowkey，有Authors列族、Inf列族
//Authors列族有：Authors：人名：单位简介
//inf:abstracts:简介
//inf:indexs:级别
//inf:isFilter:Boolean
//inf:keyword:论文关键字
//inf:paperID:ID号
//inf:publishinghouse:出版商
//inf:sortnumber:排序号
//inf:time:发表时间
public class XMLFIlesToHbase {
    //控制Hadoop各阶段
    public static void main(String args) throws Exception{
        //对Hadoop各个阶段进行设置，其中Hadoop的文件存取路径实验室服务器与科技厅不相同

    }

    //mapper阶段用于从HDFS中拿取XML文件，通过相应属性进行解析，并将解析后的结果传递至Reduce中
    public static class FileToHbaseMapper extends Mapper<LongWritable, Text, Text, Text> {

    }

    //reduce阶段用于将mapper阶段中传递过来的属性值存储到相应的Hbase表中
    public static class FileToHbaseReduce extends TableReducer<Text, Text, NullWritable>{

    }
}




