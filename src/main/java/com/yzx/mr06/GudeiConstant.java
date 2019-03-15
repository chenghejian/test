package com.yzx.mr06;

public class GudeiConstant {
	public static final String HBASE_TABLENAME="guide_table";
	public static final String HBASE_HCOUNT="hcount";
	public static final String HBASE_HCOST="hcost";
	public static final String HBASE_HREIMBURSE="hreimburse";
	public static final String HBASE_HRECOVERY="hrecovery";
	public static final String HBASE_HDAY="hday";
	public static final String HBASE_OCOUNT="ocount";
	public static final String HBASE_OCOST="ocost";
	public static final String HBASE_OREIMBURSE="oreimburse";
	public static final String HBASE_ORECOVERY="orecovery";
	public static final String HBASE_ODAY="oday";

	
	//hive 中维度表名
	public static final String HIVE_HOSPITAL_TABLENAME="hospital_dim";
	public static final String HIVE_DISEASE_TABLENAME="disease_dim";
	public static final String HIVE_HOSPITAL_DISEASE_TABLENAME="hospitalid_disease_dim";
	
	//hdfs常量
	public static final String HDFS_TMP_DIR="hdfs://master:9000/guide/tmp/";
	public static final String HDFS_HIS_DIR="hdfs://master:9000/guide/his/";	
	//linux 用来存储每天数据组上传文件的目录	
	public static final String LINUX_TMP_DIR="/tmp/guide/";
	public static final String LINUX_IP="192.168.52.252";
	public static final String LINUX_USERNAME="root";
	public static final String LINUX_PASSWORD="123456";
	
	public static final String LINUX_HIVE_SCRIPT_PATH="/root/test/guide.sh";
	//
	public static final String WINDOWS_TMP_DIR="D:\\test\\guide";
	
	
}
