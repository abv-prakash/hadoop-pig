HourlyDedup = LOAD '$DEDUP' USING PigStorage('\u0001') AS
(advid, udid, adid, handset, ccid, c_time, d_time, appid, siteid, process_time, ad_group, campaign_id, sourcetype, goal , dwimpid);
