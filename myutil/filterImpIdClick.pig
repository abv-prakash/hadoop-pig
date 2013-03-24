register /home/amitb/pig/lib/BIAnalyticsUDF.jar;
register /home/amitb/pig/lib/ivory-targen-0.1-SNAPSHOT.jar;
register /home/amitb/pig/lib/jackson-jaxrs-1.0.1.jar;
register /home/amitb/pig/lib/json-simple-1.1.jar;
register /home/amitb/pig/lib/thrift-server-0.1.jar;
register /home/amitb/pig/lib/bi-analytics-0.6.jar;
register /home/amitb/pig/lib/google-collect-1.0.jar;
register /home/amitb/pig/lib/jackson-core-asl-1.0.1.jar;
register /home/amitb/pig/lib/jackson-mapper-asl-1.0.1.jar;
register /home/amitb/pig/lib/libthrift-0.7.0.jar;
register /home/amitb/pig/lib/UDIDEncryption-1.0.jar;

DEFINE MD5 com.inmobi.adroit.pig.udidencription.MD5();
DEFINE SHA1 com.inmobi.adroit.pig.udidencription.SHA1();
DEFINE ConvertString org.inmobi.libs.parser.ConvertString();
DEFINE UDFJSONMapGet org.inmobi.libs.parser.UDFJSONMapGet();
DEFINE UDFJSONArrayGet org.inmobi.libs.parser.UDFJSONArrayGet();

click_log = load '$HourlyClick' USING TextLoader();

click_gen = foreach click_log generate FLATTEN(org.inmobi.libs.parser.SingleLineJSONUDF()) as (json: map[]);

click_data_proj = foreach click_gen generate 
ConvertString(UDFJSONMapGet((chararray)json#'rq-params', 'rq-site-inc-id', '')) as cl_site_id,
(chararray)json#'w-adincid' as cl_ad_inc_id,
(chararray)json#'w-adver' as cl_ad_version,
UDFJSONMapGet((chararray)json#'rq-params', 'rq-guid', '') as cl_impid,
(chararray)json#'terminated' as cl_terminated,
UDFJSONArrayGet((chararray)json#'handset', '0', '') as cl_handset,
UDFJSONArrayGet((chararray)json#'carrier', '0', '-') as carrier_ccid,
UDFJSONArrayGet((chararray)json#'carrier', '3', '-') as carrier_region_id,  
UDFJSONArrayGet((chararray)json#'carrier', '4', '-') as carrier_city_id,
UDFJSONMapGet((chararray)json#'rq-params', 'rq-ccid', '') as rq_ccid,
(chararray)json#'w-fraud' as cl_fraud,
UDFJSONMapGet((chararray)json#'rq-params', 'u-id', '') as cl_udid,
(chararray)json#'w-advid' as cl_adv_id,
(chararray)json#'ttime' as cl_time,
ConvertString(UDFJSONMapGet((chararray)json#'rq-headers', 'rq-user-agent', '')) as cl_ua,
UDFJSONArrayGet((chararray)json#'ad','3','') as cl_adgroup_guid,
UDFJSONArrayGet((chararray)json#'ad','4','') as cl_campaign_guid,
ConvertString(UDFJSONMapGet((chararray)json#'rq-params', 'u-gender', '')) as cl_gender,
ConvertString(UDFJSONMapGet((chararray)json#'rq-headers', 'rq-x-real-ip', '')) as cl_xrealip,
(chararray)json#'w-fraud-result' as cl_fraudset,
ConvertString(UDFJSONMapGet((chararray)json#'rq-params', 'rq-device-id', '')) as cl_device_id,
(chararray)json#'dst' as cl_demand_source_type;

click_valid = filter click_data_proj by cl_adv_id is not null and cl_adv_id != '' and cl_terminated == 'NO';

ImpListLoad = load '$List';
ImpList = foreach ImpListLoad generate $0 as impid;

J = join click_valid by cl_impid, ImpList by impid;
F = foreach J generate click_valid::cl_time,ImpList::impid,click_valid::cl_fraud,click_valid::cl_fraudset,click_valid::cl_terminated;
store F into '$OUTPUT' using PigStorage(',');

