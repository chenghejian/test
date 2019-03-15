package com.yzx.mr02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
@SuppressWarnings("all")
public class Reduce2 {

    public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String hospitalId = key.toString();
            int num = 0;
            double allBaoXiao = 0;
            double allHuaFei = 0;
            double allDay = 0;
            double goodNum = 0;
            for (Text value : values) {
                String row = value.toString();
                String[] split = row.split("\t");

                //总人数
                num++;
                //总报销费用
                allBaoXiao += Double.parseDouble(split[split.length - 1]);
                //总花费
                allHuaFei += Double.parseDouble(split[split.length - 4]);
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

            }
            //均值转化
            NumberFormat nf1 = NumberFormat.getIntegerInstance();
            nf1.setMaximumFractionDigits(2); //小数保留两位
            nf1.setGroupingUsed(false); //不显示逗号
            //人均花费
            double avgcost = allHuaFei / num;
            //人均报销费用
            double avgreimburse = Double.parseDouble(nf1.format(allBaoXiao / num));
            //人均报销比例
            double avgreproportion =Double.parseDouble(nf1.format((allBaoXiao / allHuaFei) * 100));
            //人均住院天数
            int avgday = (int)allDay / num;
            //治愈率
            double avgfinproportion =Double.parseDouble(nf1.format((goodNum / num) * 100));
            context.write(new Text(hospitalId + "\t"), new Text(num + "\t" + avgcost + "\t" + avgreimburse + "\t" + avgreproportion+"%" + "\t" + avgday + "\t" + avgfinproportion+"%"));

        }
    }


}
