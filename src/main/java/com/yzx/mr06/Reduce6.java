package com.yzx.mr06;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


@SuppressWarnings("all")
public class Reduce6 {

    public static class MyReduce6 extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String rowKey = null;
            String rowKeyy = key.toString();
            //familyname
            if (rowKeyy.startsWith("hos")) {
                rowKey = "hospitalid";
            } else if (rowKeyy.startsWith("dis")) {
                rowKey = "diseaseid";
            } else {
                rowKey = "hospitalid+ diseaseid";
            }
            // 住院总人
            double hcount = 0;
            //住院总费用
            double hcost = 0;
            //住院总报销费用
            double hreimburse = 0;
            //住院总治愈人数
            double hrecovery = 0;
            //住院总住院天数
            double hday = 0;

            // 门诊总人
            double ocount = 0;
            //门诊总费用
            double ocost = 0;
            //门诊总报销费用
            double oreimburse = 0;
            //门诊总治愈人数
            double orecovery = 0;

            //住院人均花费
            double havgcost = 0;
            //住院人均报销费用
            double havgreimburse = 0;
            //住院人均报销比例
            double havgreproportion = 0;
            //住院人均住院天数
            int havgday = 0;
            //住院治愈率
            double havgfinproportion = 0;

            //门诊人均花费
            double oavgcost = 0;
            //门诊人均报销费用
            double oavgreimburse = 0;
            //门诊人均报销比例
            double oavgreproportion = 0;
            //门诊治愈率
            double oavgfinproportion = 0;

            for (Text value : values) {
                String row = value.toString();
                String[] split = row.split("\t");
                //住院
                if ("1".equals(split[split.length - 7])) {
                    hcount++;
                    //住院总花费
                    hcost += Double.parseDouble(split[split.length - 4]);
                    //住院总报销费用
                    hreimburse += Double.parseDouble(split[split.length - 1]);
                    //总住院天数
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    try {
                        Date date1 = format.parse(split[split.length - 6]);
                        Date date2 = format.parse(split[split.length - 5]);
                        hday += (date2.getTime() - date1.getTime()) / (1000 * 60 * 60 * 24);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if ("1".equals(split[split.length - 3])) {
                        //总治愈人数
                        hrecovery++;
                    }
                    //门诊
                } else if ("2".equals(split[split.length - 7])) {
                    //门诊总人数
                    ocount++;
                    //门诊总花费
                    ocost += Double.parseDouble(split[split.length - 4]);
                    //门诊总报销花费
                    oreimburse += Double.parseDouble(split[split.length - 1]);
                    if ("1".equals(split[split.length - 3])) {
                        //总治愈人数
                        orecovery++;
                    }
                }
            }

            Table table = Util.getConn().getTable(TableName.valueOf("mr_table"));

            //获取数据
            HashMap<String, Double> map = Util.getDatas(table,rowKeyy, rowKey);
            if(map.get("hcount")!=null){
                hcount += map.get("hcount");
                hcost +=map.get("hcost");
                hreimburse +=map.get("hreimburse");
                hrecovery +=map.get("hrecovery");
                hday +=map.get("hday");
                ocount +=map.get("ocount");
                ocost +=map.get("ocost");
                oreimburse +=map.get("oreimburse");
                orecovery +=map.get("orecovery");
            }


            //均值转化
            NumberFormat nf1 = NumberFormat.getIntegerInstance();
            nf1.setMaximumFractionDigits(2); //小数保留两位
            nf1.setGroupingUsed(false); //不显示逗号
            //添加数据
            List<Put> list = new ArrayList<Put>();
            Put put = new Put(Bytes.toBytes(rowKeyy));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("hcount"), Bytes.toBytes(String.valueOf(hcount)));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("hcost"), Bytes.toBytes(String.valueOf(hcost)));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("hreimburse"), Bytes.toBytes(String.valueOf(hreimburse)));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("hrecovery"), Bytes.toBytes(String.valueOf(hrecovery)));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("hday"), Bytes.toBytes(String.valueOf(hday)));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("ocount"), Bytes.toBytes(String.valueOf(ocount)));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("oreimburse"), Bytes.toBytes(String.valueOf(oreimburse)));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("orecovery"), Bytes.toBytes(String.valueOf(orecovery)));
            put.addColumn(Bytes.toBytes(rowKey), Bytes.toBytes("ocost"), Bytes.toBytes(String.valueOf(ocost)));
            list.add(put);
            table.put(list);
            table.close();
            Util.ConnClose();

            if (hcount == 0) {
                //门诊人均花费
                 oavgcost = Double.parseDouble(nf1.format(ocost / ocount));
                //门诊人均报销费用
                 oavgreimburse = Double.parseDouble(nf1.format(oreimburse / ocount));
                //门诊人均报销比例
                 oavgreproportion = Double.parseDouble(nf1.format(oreimburse / ocost)) * 100;
                //门诊治愈率
                 oavgfinproportion = Double.parseDouble(nf1.format((orecovery / ocount) * 100));
            } else if (ocount == 0) {
                //住院人均花费
                 havgcost = Double.parseDouble(nf1.format(hcost / hcount));
                //住院人均报销费用
                 havgreimburse = Double.parseDouble(nf1.format(hreimburse / hcount));
                //住院人均报销比例
                 havgreproportion = Double.parseDouble(nf1.format(hreimburse / hcost)) * 100;
                //住院人均住院天数
                 havgday = (int) hday / (int)hcount;
                //住院治愈率
                 havgfinproportion = Double.parseDouble(nf1.format((hrecovery / hcount) * 100));
            } else {
                //住院人均花费
                 havgcost = Double.parseDouble(nf1.format(hcost / hcount));
                //住院人均报销费用
                 havgreimburse = Double.parseDouble(nf1.format(hreimburse / hcount));
                //住院人均报销比例
                 havgreproportion = Double.parseDouble(nf1.format(hreimburse / hcost)) * 100;
                //住院人均住院天数
                 havgday = (int) hday / (int)hcount;
                //住院治愈率
                 havgfinproportion = Double.parseDouble(nf1.format((hrecovery / hcount) * 100));

                //门诊人均花费
                 oavgcost = Double.parseDouble(nf1.format(ocost / ocount));
                //门诊人均报销费用
                 oavgreimburse = Double.parseDouble(nf1.format(oreimburse / ocount));
                //门诊人均报销比例
                 oavgreproportion = Double.parseDouble(nf1.format(oreimburse / ocost)) * 100;
                //门诊治愈率
                 oavgfinproportion = Double.parseDouble(nf1.format((orecovery / ocount) * 100));
            }

            context.write(new Text(rowKeyy), new Text((int)hcount + "\t"
                    + havgcost + "\t" + havgreimburse + "\t"
                    + havgreproportion + "%" + "\t" + havgday + "\t"
                    + havgfinproportion + "%" + "\t" + (int)ocount + "\t"
                    + oavgcost + "\t" + oavgreimburse + "\t"
                    + oavgreproportion + "%" + "\t" + oavgfinproportion + "%"));

            JdbcUtil.insert(rowKeyy,hcount,havgcost,havgreimburse,havgreproportion,havgday,havgfinproportion,ocount,oavgcost,oavgreimburse,oavgreproportion,oavgfinproportion);

        }
    }
}

