package com.yzx.mr05;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
@SuppressWarnings("all")
public class Reduce5 {

    public static class MyReduce5 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String tou = key.toString();
            //住院总人数
            int hcount = 0;
            //住院总花费
            double allHuaFei = 0;
            //住院总报销费用
            double allBaoXiao = 0;
            //住院总天数
            double allDay = 0;
            //住院总治愈人数
            double goodNum = 0;

            //门诊总人数
            int ocount = 0;
            //门诊总花费
            double oallcost = 0;
            //门诊总报销
            double oallbaoxiao = 0;
            //门诊总治愈人数
            double ogoodNum = 0;

            for (Text value : values) {
                String row = value.toString();
                String[] split = row.split("\t");
                //住院
                if ("1".equals(split[split.length - 7])) {
                    hcount++;
                    //住院总花费
                    allHuaFei += Double.parseDouble(split[split.length - 4]);
                    //住院总报销费用
                    allBaoXiao += Double.parseDouble(split[split.length - 1]);
                    //总住院天数
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    try {
                        Date date1 = format.parse(split[split.length - 6]);
                        Date date2 = format.parse(split[split.length - 5]);
                        allDay += (date2.getTime() - date1.getTime()) / (1000 * 60 * 60 * 24);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if ("1".equals(split[split.length - 3])) {
                        //总治愈人数
                        goodNum++;
                    }
                    //门诊
                } else if ("2".equals(split[split.length - 7])) {
                    //门诊总人数
                    ocount++;
                    //门诊总花费
                    oallcost += Double.parseDouble(split[split.length - 4]);
                    //门诊总报销花费
                    oallbaoxiao += Double.parseDouble(split[split.length - 1]);
                    if ("1".equals(split[split.length - 3])) {
                        //总治愈人数
                        ogoodNum++;
                    }
                }

            }
            //均值转化
            NumberFormat nf1 = NumberFormat.getIntegerInstance();
            nf1.setMaximumFractionDigits(2); //小数保留两位
            nf1.setGroupingUsed(false); //不显示逗号

            if (hcount == 0) {
                //住院人均花费
                double havgcost = 0;
                //住院人均报销费用
                double havgreimburse = 0;
                //住院人均报销比例
                double havgreproportion = 0;
                //住院人均住院天数
                int havgday = 0;
                //住院治愈率
                double avgfinproportion = 0;

                //门诊人均花费
                double oavgcost = Double.parseDouble(nf1.format(oallcost / ocount));
                //门诊人均报销费用
                double oavgreimburse = Double.parseDouble(nf1.format(oallbaoxiao / ocount));
                //门诊人均报销比例
                double oavgreproportion = Double.parseDouble(nf1.format(oallbaoxiao / oallcost)) * 100;
                //门诊治愈率
                double oavgfinproportion = Double.parseDouble(nf1.format((ogoodNum / ocount) * 100));

                context.write(new Text(tou), new Text(hcount + "\t"
                        + havgcost + "\t" + havgreimburse + "\t"
                        + havgreproportion + "%" + "\t" + havgday + "\t"
                        + avgfinproportion + "%" + "\t" + ocount + "\t"
                        + oavgcost + "\t" + oavgreimburse + "\t"
                        + oavgreproportion + "%" + "\t" + oavgfinproportion + "%"));

            } else if (ocount == 0) {
                //住院人均花费
                double havgcost = Double.parseDouble(nf1.format(allHuaFei / hcount));
                //住院人均报销费用
                double havgreimburse = Double.parseDouble(nf1.format(allBaoXiao / hcount));
                //住院人均报销比例
                double havgreproportion = Double.parseDouble(nf1.format(allBaoXiao / allHuaFei)) * 100;
                //住院人均住院天数
                int havgday = (int) allDay / hcount;
                //住院治愈率
                double avgfinproportion = Double.parseDouble(nf1.format((goodNum / hcount) * 100));

                //门诊人均花费
                double oavgcost = 0;
                //门诊人均报销费用
                double oavgreimburse = 0;
                //门诊人均报销比例
                double oavgreproportion = 0;
                //门诊治愈率
                double oavgfinproportion = 0;

                context.write(new Text(tou), new Text(hcount + "\t"
                        + havgcost + "\t" + havgreimburse + "\t"
                        + havgreproportion + "%" + "\t" + havgday + "\t"
                        + avgfinproportion + "%" + "\t" + ocount + "\t"
                        + oavgcost + "\t" + oavgreimburse + "\t"
                        + oavgreproportion + "%" + "\t" + oavgfinproportion + "%"));

            } else {
                //住院人均花费
                double havgcost = Double.parseDouble(nf1.format(allHuaFei / hcount));
                //住院人均报销费用
                double havgreimburse = Double.parseDouble(nf1.format(allBaoXiao / hcount));
                //住院人均报销比例
                double havgreproportion = Double.parseDouble(nf1.format(allBaoXiao / allHuaFei)) * 100;
                //住院人均住院天数
                int havgday = (int) allDay / hcount;
                //住院治愈率
                double avgfinproportion = Double.parseDouble(nf1.format((goodNum / hcount) * 100));

                //门诊人均花费
                double oavgcost = Double.parseDouble(nf1.format(oallcost / ocount));
                //门诊人均报销费用
                double oavgreimburse = Double.parseDouble(nf1.format(oallbaoxiao / ocount));
                //门诊人均报销比例
                double oavgreproportion = Double.parseDouble(nf1.format(oallbaoxiao / oallcost)) * 100;
                //门诊治愈率
                double oavgfinproportion = Double.parseDouble(nf1.format((ogoodNum / ocount) * 100));

                context.write(new Text(tou), new Text(hcount + "\t"
                        + havgcost + "\t" + havgreimburse + "\t"
                        + havgreproportion + "%" + "\t" + havgday + "\t"
                        + avgfinproportion + "%" + "\t" + ocount + "\t"
                        + oavgcost + "\t" + oavgreimburse + "\t"
                        + oavgreproportion + "%" + "\t" + oavgfinproportion + "%"));
            }


        }
    }

}
