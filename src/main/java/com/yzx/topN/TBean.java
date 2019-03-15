package com.yzx.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TBean implements WritableComparable<TBean> {

    private String province;
    private String city;
    private Integer grade;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(province);
        dataOutput.writeUTF(city);
        dataOutput.writeInt(grade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.province = dataInput.readUTF();
        this.city = dataInput.readUTF();
        this.grade = dataInput.readInt();
    }

    @Override
    public int compareTo(TBean o) {
        if(this.province.equals(o.province)){
            return this.grade.compareTo(o.grade);
        }else{
            return this.province.compareTo(o.province);
        }
    }

    public TBean() {
    }

    @Override
    public String toString() {
        return "TBean{" +
                "province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", grade=" + grade +
                '}';
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setGrade(Integer grade) {
        this.grade = grade;
    }

    public String getProvince() {
        return province;
    }

    public String getCity() {
        return city;
    }

    public Integer getGrade() {
        return grade;
    }

    public TBean(String province, String city, Integer grade) {
        this.province = province;
        this.city = city;
        this.grade = grade;
    }
}
