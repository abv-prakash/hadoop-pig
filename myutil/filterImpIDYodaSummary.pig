yoda_summary = load '$YodaSummary' using PigStorage('\u0001') as 
 (adv_id, cl_udid, cl_ad_inc_id, cl_handset, rq_ccid, cl_time, be_time,
 be_app_id,cl_site_id,processed_time,cl_adgroup_guid,cl_campaign_guid,cl_impid,
 be_udid,cl_fraud,cl_ua,cl_gender,cl_xrealip,cl_fraudset,cl_device_id,cl_hash,
 be_hash,carrier_ccid,source_type, goal,be_impid,be_terminated,joined, duplicate,
 cl_demand_source_type, carrier_region_id, carrier_city_id);

yoda_summary_filt = filter yoda_summary by 
(be_impid == '7f1f417e-013c-1000-ef3e-3fcf1e2d0062' or 
be_impid == '7bc9506d-013c-1000-e1bb-3e9a1e460061' or
be_impid == '81b91470-013c-1000-eac6-3ecc1e610062' or
be_impid == '855c3aa6-013c-1000-cb3e-3f181e2d0064' or
be_impid == '88c6b72b-013c-1000-c63e-3eb41e2d0064' or
be_impid == '89749d0e-013c-1000-f018-3e9e1e6d0065' or
be_impid == '8d178ec4-013c-1000-ddbd-3eef1e610044' or
be_impid == '8e038a2c-013c-1000-c5b0-3e901e6100c4' or
be_impid == '6571d570-013c-1000-eabb-3ee61e460060' or
be_impid == '8ed339b1-013c-1000-dbb7-3eee1e6100c5' or
be_impid == '92c3d83a-013c-1000-e0ae-3e931e6100c4' or
be_impid == '9319513d-013c-1000-edaa-40761e610041' or
be_impid == '9328421a-013c-1000-e7cb-3ed31e610065' or
be_impid == '908ab492-013c-1000-deca-3f3a1e610061' or
be_impid == '99158082-013c-1000-e8ce-3ecb1e610047' or
be_impid == '99c81ac5-013c-1000-eeb6-3f2f1e610060');

store yoda_summary_filt into '$OUTPUT';
