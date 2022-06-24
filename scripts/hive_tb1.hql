create table if not exists Project_1.mang_tbl_cust
(
custid int,
username string,
quote_count string,
ip string,
entry_time string,
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
updated_time string
)
row format delimited fields terminated by ',';
truncate table Project_1.mang_tbl_cust;
load data inpath '/user/saif/HFS/Output/Project_1/imp_data' into table Project_1.mang_tbl_cust;
