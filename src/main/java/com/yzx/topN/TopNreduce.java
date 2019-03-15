package com.yzx.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TopNreduce {

    public static class Treduce extends Reducer<Text,TBean,Text,TBean>{
        @Override
        protected void reduce(Text key, Iterable<TBean> values, Context context) throws IOException, InterruptedException {

            List<TBean> list = new ArrayList();

            for(TBean tt:values){
                TBean tb2 = new TBean(tt.getProvince(),tt.getCity(),tt.getGrade());
                list.add(tb2);
            }

            Collections.sort(list);
            int topn = context.getConfiguration().getInt("top.n", 3);
            for(int i=0;i<topn;i++){
                context.write(key,list.get(i));
            }


        }
    }

}
