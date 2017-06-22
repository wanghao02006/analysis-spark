package com.leiyu.sparkproject.dao;

import com.leiyu.sparkproject.domain.SessionAggrStat;

/**
*session聚合统计模块DAO接口
 *  @author wh
*/
public interface ISessionAggrStatDAO {

    /**
     * 插入聚合函数
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);
}
