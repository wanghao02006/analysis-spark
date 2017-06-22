package com.leiyu.sparkproject.spark;

import com.leiyu.sparkproject.constants.Constants;
import com.leiyu.sparkproject.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * Created by wh on 2017/6/21.
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {
    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1,r2);
    }

    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    private String add(String r1, String r2) {
        if(StringUtils.isEmpty(r1)){
            return r2;
        }

        String oldValue = StringUtils.getFieldFromConcatString(r1,"\\|",r2);
        if(oldValue != null){
            int newValue = Integer.valueOf(oldValue) + 1;
            return StringUtils.setFieldInConcatString(r1,"\\|",r2,String.valueOf(newValue));
        }
        return r1;
    }
}
