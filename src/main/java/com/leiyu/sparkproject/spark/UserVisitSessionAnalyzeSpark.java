package com.leiyu.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.leiyu.sparkproject.conf.ConfigurationManager;
import com.leiyu.sparkproject.constants.Constants;
import com.leiyu.sparkproject.dao.ISessionAggrStatDAO;
import com.leiyu.sparkproject.dao.ITaskDAO;
import com.leiyu.sparkproject.dao.impl.DAOFactory;
import com.leiyu.sparkproject.domain.SessionAggrStat;
import com.leiyu.sparkproject.domain.Task;
import com.leiyu.sparkproject.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * 用户行为分析类
 */
public class UserVisitSessionAnalyzeSpark {


    public static void main(String[] args) {
        //1、创建SparkConf，并用本地模式运行
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        // 2、创建JavaSparkContext，作为计算入口
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3、创建SqlContext，用于数据分析
        SQLContext sqlContext = getSQLContext(sc.sc());

        // 4、生成模拟数据，只在本地模式生效
        mockData(sc,sqlContext);

        // 5、加载分析任务以及分析任务需要参数
        // 5.1、创建TaskDAO，用于从数据库加载任务信息
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 5.2、根据TaskID加载任务信息以及分析参数
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //6、从user_visit_action表中，查询指定日期范围内行为数据，以供后续分析
        //数据来源：a、Hive数据库，b、mock模拟数据（仅适用于local模式）
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext,taskParam);

        // 7、根据session聚合数据，返回RDD中数据Tuple._1:sessionid,Tuple._2：聚合信息拼接字符串
        JavaPairRDD<String,String> sessionid2AggrInfoRDD = aggregateBySession(sqlContext,actionRDD);

        // Action数据计数器，用于聚合Action次数
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SessionAggrStatAccumulator());

        // 8、根据Task中参数过滤Action与User连接数据，同时对Action进行计数
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
                filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam,sessionAggrStatAccumulator);

        /// 9、保存计算数据，该部分内容必须在执行action后，否则将无法进行
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskid());

        sc.close();
    }

    /**
     * 创建SQLContext，根据配置文件判断，如果是Local模式，则返回SQLContext。
     * 否则返回HiveContext。
     *
     * @param sc
     * @return
     */
    public static SQLContext getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            return new SQLContext(sc);
        }else{
            return new HiveContext(sc);
        }
    }

    /**
     * 模拟测试数据，只在本地模式生效
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            MockUserVisitSessionAnalyzeSparkData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定时间范围内用户访问行为数据
     * @param sqlContext
     * @param taskParam
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext,JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql = "select * "
                + " from user_visit_action "
                + " where date >= '" + startDate + "'"
                +" and date <= '" + endDate + "'";
        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合
     * @param sqlContext
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String,String> aggregateBySession(SQLContext sqlContext,JavaRDD<Row> actionRDD){

        //1、将JavaRDD<Row>映射为JavaPairRDD,其中Row中第三项为sessionid
        JavaPairRDD<String,Row> sessionid2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2),row);
            }
        });

        // 2、通过groupByKey进行分组聚合，将sessionId对应的操作Row聚合到一起
        JavaPairRDD<String,Iterable<Row>> sessionid2GroupActionRDD = sessionid2ActionRDD.groupByKey();

        // 3、根据用户id聚合数据，不再使用sessionid，而是使用userid作为聚合参数
        JavaPairRDD<Long,String> userid2PartAggrInfoRDD = sessionid2GroupActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                        String sessionid = stringIterableTuple2._1;
                        Iterator<Row> iterator = stringIterableTuple2._2.iterator();

                        //记录检索关键词，使用逗号分隔
                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        //记录点击事件，使用逗号分隔
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        //用户id，一个session对应一个用户id
                        Long userid = null;

                        //session开始时间
                        Date startTime = null;
                        //session结束时间
                        Date endTime = null;
                        //session中步长
                        int stepLength = 0 ;

                        // 3.1、循环遍历session对应所有Action
                        while (iterator.hasNext()){
                            Row row = iterator.next();

                            // 加载用户信息
                            if(userid == null){
                                userid = row.getLong(1);
                            }

                            // 3.2、获取并整理（拼接）用户检索词以及点击事件
                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getLong(6);

                            if(StringUtils.isNotEmpty(searchKeyword)){
                                if(!searchKeywordsBuffer.toString().contains(searchKeyword)){
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }

                            if(clickCategoryId != null){
                                if(!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 3.3、获取Action Time，用于计算开始时间与结束时间，并计算访问时长
                            Date actionTime = DateUtils.parseTime(row.getString(4));

                            if(null == startTime){
                                startTime = actionTime;
                            }
                            if(null == endTime){
                                endTime = actionTime;
                            }

                            if(actionTime.before(startTime)){
                                startTime = actionTime;
                            }
                            if(actionTime.after(endTime)){
                                endTime = actionTime;
                            }

                            // 3.4、计算访问步长
                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 3.5、计算访问时长
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        // 3.6、返回拼接后字符串
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength;
                        return new Tuple2<Long, String>(userid,partAggrInfo);
                    }
                });

        // 4、获取用户信息，并且与聚合Action连接查询
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        // 4.1、将用户数据RDD转为根据用户id聚合的JavaPairRDD
        JavaPairRDD<Long,Row> userid2InfoRdd = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0),row);
            }
        });

        // 4.2、聚合Action数据与用户数据聚合
        JavaPairRDD<Long,Tuple2<String,Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRdd);

        // 4.3、拼接用户信息与Action信息，并转为根据Sessionid聚合
        JavaPairRDD<String,String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> longTuple2Tuple2) throws Exception {
                String partAggrInfo = longTuple2Tuple2._2._1;
                Row userInfoRow = longTuple2Tuple2._2._2;
                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city =userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<String, String>(sessionid,fullAggrInfo);
            }
        });

        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 根据task中param过滤Action数据，并对Action进行计数
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @param sessionAggrStatAccumulator
     * @return
     */
    private static JavaPairRDD<String,String> filterSessionAndAggrStat(
            JavaPairRDD<String,String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator){
        // 1、组装过滤条件
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")){
            _parameter = _parameter.substring(0,_parameter.length() -1);
        }

        final String parameter = _parameter;

        //2、对数据进行过滤
        JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        String aggrInfo = stringStringTuple2._2;

                        //2.1、针对各个条件过滤，不符合要求返回false，否则继续验证其他条件
                        if(!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
                            return false;
                        }

                        if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter,Constants.PARAM_PROFESSIONALS)){
                            return false;
                        }

                        if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,Constants.PARAM_CITIES)){
                            return false;
                        }

                        if(!ValidUtils.equal(aggrInfo,Constants.FIELD_SEX,parameter,Constants.PARAM_SEX)){
                            return false;
                        }

                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        if(!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,parameter,Constants.PARAM_CATEGORY_IDS)){
                            return false;
                        }

                        //2.2、如果满足所有条件，则总数加1
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 2.3、计算访问时长以及访问步长，然后针对对应数据计数
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo,"\\|",Constants.FIELD_VISIT_LENGTH
                        ));
                        long stepLenth = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo,"\\|",Constants.FIELD_STEP_LENGTH
                        ));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLenth);
                        return true;
                    }

                    /**
                     * 访问时长计数
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if(visitLength >=1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if(visitLength >=4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if(visitLength >=7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if(visitLength >=10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if(visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if(visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if(visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if(visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if(visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if(stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if(stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if(stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if(stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if(stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if(stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                });
        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * 将各个计数器中值存入mysql数据库
     * @param value
     * @param taskid
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

}
