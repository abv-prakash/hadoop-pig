yoda_summary = load '$YodaSummary' using PigStorage('\u0001') as 
 (adv_id, cl_udid, cl_ad_inc_id, cl_handset, rq_ccid, cl_time, be_time,
 be_app_id,cl_site_id,processed_time,cl_adgroup_guid,cl_campaign_guid,cl_impid,
 be_udid,cl_fraud,cl_ua,cl_gender,cl_xrealip,cl_fraudset,cl_device_id,cl_hash,
 be_hash,carrier_ccid,source_type, goal,be_impid,be_terminated,joined, duplicate,
 cl_demand_source_type, carrier_region_id, carrier_city_id);

yoda_summary_filt = filter yoda_summary by (cl_impid == 'b94af37e-013c-1000-c166-3f0d1e9a00c0') and joined == '1' ;

store yoda_summary_filt into '$OUTPUT' using PigStorage('\u0001');
