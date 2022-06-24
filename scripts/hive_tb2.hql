create table if not exists Project_1.cust_stagging(
custid int,
username string,
quote_count string,
ip string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code int,
day int,
updated_time string,
month string,
year string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
insert overwrite table Project_1.cust_stagging select custid,username,quote_count,ip,prp_1,
prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,
http_info,status_code,
cast(split(entry_time,"[/:]")[0] as int) day,
updated_time string,
split(entry_time,"[/:]")[1] year,
split(entry_time,"[/:]")[2] month from Project_1.mang_tbl_cust;
