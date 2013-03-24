HourlyDedup = LOAD '$DEDUP' USING PigStorage('\u0001') AS 
(advid, udid, adid, handset, ccid, c_time, d_time, appid, siteid, process_time, ad_group, campaign_id, sourcetype, goal , dwimpid);
DEDUP_FILT = filter HourlyDedup by ad_group == '0e2c29744b834faaab2001fab457ae73';
STORE DEDUP_FILT INTO '$OUTPUT';
