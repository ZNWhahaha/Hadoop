package com.filestohbase.hbase;

import java.util.List;


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
public class HbaseItem {

    String title;
    String time;
    String sortnumber;
    String fundsproject;
    String abstracts;
    String organization;
    String paperid;
    String keyword;

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getAutors() {
        return autors;
    }

    public void setAutors(String autors) {
        this.autors = autors;
    }

    public String getPublishinghouse() {
        return publishinghouse;
    }

    public void setPublishinghouse(String publishinghouse) {
        this.publishinghouse = publishinghouse;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    String autors;
    String publishinghouse;
    String index;

    public String getPaperid() {
        return paperid;
    }

    public void setPaperid(String paperid) {
        this.paperid = paperid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }


    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getSortnumber() {
        return sortnumber;
    }

    public void setSortnumber(String sortnumber) {
        this.sortnumber = sortnumber;
    }

    public String getFundsproject() {
        return fundsproject;
    }

    public void setFundsproject(String fundsproject) {
        this.fundsproject = fundsproject;
    }

    public String getAbstracts() {
        return abstracts;
    }

    public void setAbstracts(String abstracts) {
        this.abstracts = abstracts;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }



}
