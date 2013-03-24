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

-- load current hours beacon log
beacon_log = load '$HourlyBeacon' using TextLoader();

beacon_gen = foreach beacon_log generate FLATTEN(org.inmobi.libs.parser.SingleLineJSONUDF()) as (json: map[]);

beacon_data_proj = foreach beacon_gen generate
((chararray)json#'adv-id-key' is null or (chararray)json#'adv-id-key' == '' ? '-' : TRIM((chararray)json#'adv-id-key')) as be_adv_id, 

((chararray)json#'app-id-key' is null or (chararray)json#'app-id-key' == '' ? '-' : TRIM((chararray)json#'app-id-key')) as be_app_id, 
((chararray)json#'udid-key' is null or (chararray)json#'udid-key' == '' ? '-' : TRIM((chararray)json#'udid-key')) as be_udid,
-- beacon_time is known as inittime in database
(chararray)json#'ttime' as be_time, 
(chararray)json#'terminated' as be_terminated,
((chararray)json#'impid' is null or (chararray)json#'impid' == '' ? '-' : TRIM((chararray)json#'impid')) as be_impid, 
TRIM((chararray)json#'src') as source_type,
-- adx dosen't send us goal hence we need to annotate it otherwise it won't match during deduplication
((chararray)json#'goal' is null or (chararray)json#'goal' == '' ? '-' : TRIM((chararray)json#'goal')) as goal,
ConvertString(UDFJSONMapGet((chararray)json#'arg-list', 'sdtappid','')) as sdtappid;

beacon_valid = filter beacon_data_proj by be_terminated == 'NO' and 
(sdtappid == '83ef7cb7-0c48-4373-9d34-dc3de089c0dd' or sdtappid == 'e70b7d14-acde-4e1f-98db-a3fb128add96' or sdtappid == '7fc26195-1bd0-4a88-be7b-89f62c403a6a'
or sdtappid =='378bdefe-0c5a-4b34-87be-02d615a69f71' or sdtappid == 'e69a199a-0ae7-41b9-8448-98128d281aa1') ;

store beacon_valid into '$OUTPUT';


