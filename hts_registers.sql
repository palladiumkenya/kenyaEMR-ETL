

-- ETL for HTS forms
	-- hts initial
	-- hts confirmation
	-- linkage

		/*HTS_INITIAL_TEST = "402dc5d7-46da-42d4-b2be-f43ea4ad87b0";
		HTS_CONFIRMATORY_TEST = "b08471f6-0892-4bf7-ab2b-bf79797b8ea4";
		REFERRAL_AND_LINKAGE = "050a7f12-5c52-4cad-8834-863695af335d";*/


drop table if exists kenyaemr_etl.etl_hts_test;
create table kenyaemr_etl.etl_hts_test (
patient_id INT(11) not null,
visit_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL primary key,
encounter_uuid CHAR(32) NOT NULL,
encounter_location INT(11) NOT NULL,
creator INT(11) NOT NULL,
date_created DATE NOT NULL,
visit_date DATE,
test_type VARCHAR(50) DEFAULT NULL,
population_type VARCHAR(50),
key_population_type VARCHAR(50),
ever_tested_for_hiv VARCHAR(10),
months_since_last_test INT(11),
patient_disabled VARCHAR(50),
disability_type VARCHAR(50),
patient_consented VARCHAR(50) DEFAULT NULL,
client_tested_as VARCHAR(50),
test_strategy VARCHAR(50),
test_1_kit_name VARCHAR(50),
test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
test_1_kit_expiry DATE DEFAULT NULL,
test_1_result VARCHAR(50) DEFAULT NULL,
test_2_kit_name VARCHAR(50),
test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
test_2_kit_expiry DATE DEFAULT NULL,
test_2_result VARCHAR(50) DEFAULT NULL,
final_test_result VARCHAR(50) DEFAULT NULL,
patient_given_result VARCHAR(50) DEFAULT NULL,
couple_discordant VARCHAR(100) DEFAULT NULL,
tb_screening VARCHAR(20) DEFAULT NULL,
patient_had_hiv_self_test VARCHAR(50) DEFAULT NULL,
provider_name VARCHAR(50) DEFAULT NULL,
remarks VARCHAR(50) DEFAULT NULL,
voided INT(11),
index(patient_id),
index(visit_id),
index(tb_screening),
index(visit_date),
index(population_type),
index(test_type),
index(final_test_result),
index(couple_discordant),
index(test_1_kit_name),
index(test_2_kit_name)
);


-- populate hts test table

insert into kenyaemr_etl.etl_hts_test (
patient_id,
visit_id,
encounter_id,
encounter_uuid,
encounter_location,
creator,
date_created,
visit_date,
test_type,
population_type,
key_population_type,
ever_tested_for_hiv,
months_since_last_test,
patient_disabled,
disability_type,
patient_consented,
client_tested_as,
test_strategy,
test_1_kit_name,
test_1_kit_lot_no,
test_1_kit_expiry,
test_1_result,
test_2_kit_name,
test_2_kit_lot_no,
test_2_kit_expiry,
test_2_result,
final_test_result,
patient_given_result,
couple_discordant,
tb_screening,
patient_had_hiv_self_test ,
provider_name,
remarks,
voided
)
select
e.patient_id,
e.visit_id,
e.encounter_id,
e.uuid,
ef.uuid,
e.location_id,
e.creator,
e.date_created,
e.encounter_datetime as visit_date,
max(if(o.concept_id=162084 and o.value_coded=162080 and ef.uuid != "b08471f6-0892-4bf7-ab2b-bf79797b8ea4","Initial","confirmation")) as test_type ,
max(if(o.concept_id=164930,(case o.value_coded when 164928 then "General Population" when 164929 then "Key Population" else "" end),null)) as population_type,
max(if(o.concept_id=160581,(case o.value_coded when 105 then "People who inject drugs" when 160578 then "Men who have sex with men" when 160579 then "Female sex worker" else "" end),null)) as key_population_type,
max(if(o.concept_id=164401,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as ever_tested_for_hiv,
null as months_since_last_test,
max(if(o.concept_id=164951,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_disabled,
max(if(o.concept_id=162558,(case o.value_coded when 120291 then "Deaf" when 147215 then "Blind" when 151342 then "Mentally Challenged" when 164538 then "Physically Challenged" when 5622 then "Other" else "" end),null)) as disability_type,
max(if(o.concept_id=1710,(case o.value_boolean when 1 then "Yes" when 0 then "No" else "" end),null)) as patient_consented,
max(if(o.concept_id=164959,(case o.value_coded when 164957 then "Individual" when 164958 then "Couple" else "" end),null)) as client_tested_as,
max(if(o.concept_id=164956,(
  case o.value_coded
  when 164163 then "Provider Initiated Testing(PITC)"
  when 164953 then "Non Provider Initiated Testing"
  when 164954 then "Integrated VCT Center"
  when 164955 then "Stand Alone VCT Center"
  when 159938 then "Home Based Testing"
  when 159939 then "Mobile Outreach HTS"
  when 5622 then "Other"
  else ""
  end ),null)) as test_strategy,
max(if(t.test_1_result is not null, t.kit_name, "")) as test_1_kit_name,
max(if(t.test_1_result is not null, t.lot_no, "")) as test_1_kit_lot_no,
max(if(t.test_1_result is not null, t.expiry_date, "")) as test_1_kit_expiry,
max(if(t.test_1_result is not null, t.test_1_result, "")) as test_1_result,
max(if(t.test_2_result is not null, t.kit_name, "")) as test_2_kit_name,
max(if(t.test_2_result is not null, t.lot_no, "")) as test_2_kit_lot_no,
max(if(t.test_2_result is not null, t.expiry_date, "")) as test_2_kit_expiry,
max(if(t.test_2_result is not null, t.test_2_result, "")) as test_2_result,
max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
max(if(o.concept_id=6096,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as couple_discordant,
max(if(o.concept_id=1659,(case o.value_coded when 1660 then "No TB signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "Not done" when 1111 then "On TB Treatment"  else "" end),null)) as tb_screening,
max(if(o.concept_id=164952,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_had_hiv_self_test,
max(if(o.concept_id=163042,o.value_text,null)) as remarks,
e.voided
from encounter e
inner join
(
  select form_id, uuid, name from form where uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
) ef on ef.form_id=e.form_id
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (162084, 164930, 160581, 164401, 164951, 162558, 1710, 164959, 164956,
                                                                                 159427, 164848, 6096, 1659, 164952, 163042)
inner join (
             select
               o.person_id,
               o.encounter_id,
               o.obs_group_id,
               max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
               max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
               max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "Uni-Gold" else "" end),null)) as kit_name ,
               max(if(o.concept_id=164964,o.value_text,null)) as lot_no,
               max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
             from obs o inner join encounter e on e.encounter_id = o.encounter_id
               inner join
               (
                 select form_id, uuid, name from form where uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
               ) ef on ef.form_id=e.form_id
             where o.concept_id in (1040, 1326, 164962, 164964, 162502)
             group by e.encounter_id, o.obs_group_id
             order by e.encounter_id, o.obs_group_id
           ) t on e.encounter_id = t.encounter_id
group by e.encounter_id;


-- 
select 
e.patient_id,
e.visit_id,
e.encounter_id,
e.uuid,
e.location_id,
e.creator,
e.date_created,
e.encounter_datetime as visit_date,
max(if(o.concept_id=162084 and o.value_coded=162080,"Initial","confirmation")) as test_type ,
max(if(o.concept_id=164930,(case o.value_coded when 164928 then "General Population" when 164929 then "Key Population" else "" end),null)) as population_type,
e.voided
from encounter e 
inner join 
(
	select form_id, uuid, name from form where uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
) ef on ef.form_id=e.form_id
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (162084, 164930)
group by e.encounter_id


-- extracting test details

select 
o.person_id,
o.encounter_id,
o.obs_group_id,
max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "Uni-Gold" else "" end),null)) as kit_name ,
max(if(o.concept_id=164964,o.value_text,null)) as lot_no,
max(if(o.concept_id=162502,o.value_datetime,null)) as expiry_date
from obs o inner join encounter e on e.encounter_id = o.encounter_id
inner join 
(
	select form_id, uuid, name from form where uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
) ef on ef.form_id=e.form_id
where o.concept_id in (1040, 1326, 164962, 164964, 162502)
group by e.encounter_id, o.obs_group_id
order by e.encounter_id, o.obs_group_id


-- 
create table kenyaemr_etl.etl_hts_referral_and_linkage (
patient_id INT(11) not null primary key,
encounter_id INT(11) NOT NULL,
visit_date DATE,
tracing_type VARCHAR(50),
tracing_status VARCHAR(10),
ccc_number INT(11),
facility_linked_to VARCHAR(50),
provider_handed_to VARCHAR(50)
index(patient_id),
index(visit_date),
index(tracing_type),
index(tracing_status)
);
