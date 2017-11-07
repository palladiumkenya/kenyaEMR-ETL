
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_patient_demographics$$
CREATE PROCEDURE sp_populate_etl_patient_demographics()
BEGIN
-- initial set up of etl_patient_demographics table
SELECT "Processing patient demographics data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_patient_demographics(
patient_id,
given_name,
middle_name,
family_name,
Gender,
DOB,
dead,
voided,
death_date
)
select 
p.person_id,
p.given_name,
p.middle_name,
p.family_name,
p.gender,
p.birthdate,
p.dead,
p.voided,
p.death_date
FROM (
select 
p.person_id,
pn.given_name,
pn.middle_name,
pn.family_name,
p.gender,
p.birthdate,
p.dead,
p.voided,
p.death_date
from person p 
left join patient pa on pa.patient_id=p.person_id
left join person_name pn on pn.person_id = p.person_id and pn.voided=0
GROUP BY p.person_id
) p
ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name=p.middle_name, family_name=p.family_name;


-- update etl_patient_demographics with patient attributes: birthplace, citizenship, mother_name, phone number and kin's details
update kenyaemr_etl.etl_patient_demographics d 
left outer join 
(
select 
pa.person_id,  
max(if(pat.uuid='8d8718c2-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as birthplace,
max(if(pat.uuid='8d871afc-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as citizenship,
max(if(pat.uuid='8d871d18-c2cc-11de-8d13-0010c6dffd0f', pa.value, null)) as Mother_name,
max(if(pat.uuid='b2c38640-2603-4629-aebd-3b54f33f1e3a', pa.value, null)) as phone_number,
max(if(pat.uuid='342a1d39-c541-4b29-8818-930916f4c2dc', pa.value, null)) as next_of_kin_contact,
max(if(pat.uuid='d0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5', pa.value, null)) as next_of_kin_relationship,
max(if(pat.uuid='7cf22bec-d90a-46ad-9f48-035952261294', pa.value, null)) as next_of_kin_address,
max(if(pat.uuid='830bef6d-b01f-449d-9f8d-ac0fede8dbd3', pa.value, null)) as next_of_kin_name,
max(if(pat.uuid='b8d0b331-1d2d-4a9a-b741-1816f498bdb6', pa.value, null)) as email_address
from person_attribute pa
inner join
(
select 
pat.person_attribute_type_id,
pat.name,
pat.uuid
from person_attribute_type pat
where pat.retired=0
) pat on pat.person_attribute_type_id = pa.person_attribute_type_id 
and pat.uuid in (
	'8d8718c2-c2cc-11de-8d13-0010c6dffd0f', -- birthplace
	'8d871afc-c2cc-11de-8d13-0010c6dffd0f', -- citizenship
	'8d871d18-c2cc-11de-8d13-0010c6dffd0f', -- mother's name
	'b2c38640-2603-4629-aebd-3b54f33f1e3a', -- telephone contact
	'342a1d39-c541-4b29-8818-930916f4c2dc', -- next of kin's contact
	'd0aa9fd1-2ac5-45d8-9c5e-4317c622c8f5', -- next of kin's relationship
	'7cf22bec-d90a-46ad-9f48-035952261294', -- next of kin's address
	'830bef6d-b01f-449d-9f8d-ac0fede8dbd3', -- next of kin's name
	'b8d0b331-1d2d-4a9a-b741-1816f498bdb6' -- email address

	)
where pa.voided=0
group by pa.person_id
) att on att.person_id = d.patient_id
set d.phone_number=att.phone_number, 
	d.next_of_kin=att.next_of_kin_name,
	d.next_of_kin_relationship=att.next_of_kin_relationship,
	d.next_of_kin_phone=att.next_of_kin_contact,
	d.phone_number=att.phone_number,
	d.birth_place = att.birthplace,
	d.citizenship = att.citizenship,
	d.email_address=att.email_address;


update kenyaemr_etl.etl_patient_demographics d
join (select pi.patient_id,
max(if(pit.uuid='05ee9cf4-7242-4a17-b4d4-00f707265c8a',pi.identifier,null)) as upn,
max(if(pit.uuid='d8ee3b8c-a8fc-4d6b-af6a-9423be5f8906',pi.identifier,null)) district_reg_number,
max(if(pit.uuid='c4e3caca-2dcc-4dc4-a8d9-513b6e63af91',pi.identifier,null)) Tb_treatment_number,
max(if(pit.uuid='b4d66522-11fc-45c7-83e3-39a1af21ae0d',pi.identifier,null)) Patient_clinic_number,
max(if(pit.uuid='49af6cdc-7968-4abb-bf46-de10d7f4859f',pi.identifier,null)) National_id,
max(if(pit.uuid='0691f522-dd67-4eeb-92c8-af5083baf338',pi.identifier,null)) Hei_id
from patient_identifier pi
join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
where voided=0
group by pi.patient_id) pid on pid.patient_id=d.patient_id
set d.unique_patient_no=pid.UPN, 
	d.national_id_no=pid.National_id,
	d.patient_clinic_number=pid.Patient_clinic_number,
    d.hei_no=pid.Hei_id,
    d.Tb_no=pid.Tb_treatment_number,
    d.district_reg_no=pid.district_reg_number
;

update kenyaemr_etl.etl_patient_demographics d
join (select o.person_id as patient_id,
max(if(o.concept_id in(1054),cn.name,null))  as marital_status,
max(if(o.concept_id in(1712),cn.name,null))  as education_level
from obs o
join concept_name cn on cn.concept_id=o.value_coded and cn.concept_name_type='FULLY_SPECIFIED'
and cn.locale='en'
where o.concept_id in (1054,1712) and o.voided=0
group by person_id) pstatus on pstatus.patient_id=d.patient_id
set d.marital_status=pstatus.marital_status,
d.education_level=pstatus.education_level;

END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_enrollment$$
CREATE PROCEDURE sp_populate_etl_hiv_enrollment()
BEGIN
-- populate patient_hiv_enrollment table
-- uuid: de78a6be-bfc5-4634-adc3-5f1a280455cc
SELECT "Processing HIV Enrollment data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_hiv_enrollment (
patient_id,
uuid,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
patient_type,
date_first_enrolled_in_care,
entry_point,
transfer_in_date,
facility_transferred_from,
district_transferred_from,
date_started_art_at_transferring_facility,
date_confirmed_hiv_positive,
facility_confirmed_hiv_positive,
arv_status,
name_of_treatment_supporter,
relationship_of_treatment_supporter,
treatment_supporter_telephone,
treatment_supporter_address,
voided
)
select 
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_datetime as visit_date,
e.location_id,
e.encounter_id,
e.creator,
e.date_created,
max(if(o.concept_id=164932,o.value_coded,null)) as patient_type ,
max(if(o.concept_id=160555,o.value_datetime,null)) as date_first_enrolled_in_care ,
max(if(o.concept_id=160540,o.value_coded,null)) as entry_point,
max(if(o.concept_id=160534,o.value_datetime,null)) as transfer_in_date,
max(if(o.concept_id=160535,o.value_text,null)) as facility_transferred_from,
max(if(o.concept_id=161551,o.value_text,null)) as district_transferred_from,
max(if(o.concept_id=159599,o.value_datetime,null)) as date_started_art_at_transferring_facility,
max(if(o.concept_id=160554,o.value_datetime,null)) as date_confirmed_hiv_positive,
max(if(o.concept_id=160632,o.value_text,null)) as facility_confirmed_hiv_positive,
max(if(o.concept_id=160533,o.value_boolean,null)) as arv_status,
max(if(o.concept_id=160638,o.value_text,null)) as name_of_treatment_supporter,
max(if(o.concept_id=160640,o.value_coded,null)) as relationship_of_treatment_supporter,
max(if(o.concept_id=160642,o.value_text,null)) as treatment_supporter_telephone ,
max(if(o.concept_id=160641,o.value_text,null)) as treatment_supporter_address,
e.voided
from encounter e 
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where uuid='de78a6be-bfc5-4634-adc3-5f1a280455cc'
) et on et.encounter_type_id=e.encounter_type
join patient p on p.patient_id=e.patient_id and p.voided=0
left outer join obs o on o.encounter_id=e.encounter_id 
	and o.concept_id in (160555,160540,160534,160535,161551,159599,160554,160632,160533,160638,160640,160642,160641)
where e.voided=0
group by e.patient_id, e.encounter_id;
SELECT "Completed processing HIV Enrollment data ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;


-- ------------- populate etl_hiv_followup--------------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_followup$$
CREATE PROCEDURE sp_populate_etl_hiv_followup()
BEGIN
SELECT "Processing HIV Followup data ", CONCAT("Time: ", NOW());
INSERT INTO kenyaemr_etl.etl_patient_hiv_followup(
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
visit_scheduled,
person_present,
weight,
systolic_pressure,
diastolic_pressure,
height,
temperature,
pulse_rate,
respiratory_rate,
oxygen_saturation,
muac,
nutritional_status,
population_type,
key_population_type,
who_stage,
presenting_complaints,
clinical_notes,
on_anti_tb_drugs,
on_ipt,
ever_on_ipt,
spatum_smear_ordered,
chest_xray_ordered,
genexpert_ordered,
spatum_smear_result,
chest_xray_result,
genexpert_result,
referral,
clinical_tb_diagnosis,
contact_invitation,
evaluated_for_ipt,
has_known_allergies,
has_chronic_illnesses_cormobidities,
has_adverse_drug_reaction,
pregnancy_status,
wants_pregnancy,
pregnancy_outcome,
anc_number,
expected_delivery_date,
last_menstrual_period,
gravida,
parity,
full_term_pregnancies,
abortion_miscarriages,
family_planning_status,
family_planning_method,
reason_not_using_family_planning,
tb_status,
tb_treatment_no,
ctx_adherence,
ctx_dispensed,
dapsone_adherence,
dapsone_dispensed,
inh_dispensed,
arv_adherence,
poor_arv_adherence_reason,
poor_arv_adherence_reason_other,
pwp_disclosure,
pwp_partner_tested,
condom_provided,
screened_for_sti,
cacx_screening, 
sti_partner_notification,
at_risk_population,
system_review_finding,
next_appointment_date,
next_appointment_reason,
differentiated_care,
voided
)
select 
e.patient_id,
e.visit_id,
date(e.encounter_datetime) as visit_date,
e.location_id,
e.encounter_id as encounter_id,
e.creator,
e.date_created as date_created,
max(if(o.concept_id=1246,o.value_coded,null)) as visit_scheduled ,
max(if(o.concept_id=161643,o.value_coded,null)) as person_present,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_pressure,
max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_pressure,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
max(if(o.concept_id=163300,o.value_coded,null)) as nutritional_status, 
max(if(o.concept_id=164930,o.value_coded,null)) as population_type, 
max(if(o.concept_id=160581,o.value_coded,null)) as key_population_type, 
max(if(o.concept_id=5356,o.value_coded,null)) as who_stage ,
max(if(o.concept_id=1154,o.value_coded,null)) as presenting_complaints ,
max(if(o.concept_id=160430,o.value_text,null)) as clinical_notes ,
max(if(o.concept_id=164948,o.value_coded,null)) as on_anti_tb_drugs ,
max(if(o.concept_id=164949,o.value_coded,null)) as on_ipt ,
max(if(o.concept_id=164950,o.value_coded,null)) as ever_on_ipt ,
max(if(o.concept_id=1271 and o.value_coded = 307,1065,1066)) as spatum_smear_ordered ,
max(if(o.concept_id=1271 and o.value_coded = 12 ,1065,1066)) as chest_xray_ordered ,
max(if(o.concept_id=1271 and o.value_coded = 162202,1065,1066)) as genexpert_ordered ,
max(if(o.concept_id=307,o.value_coded,null)) as spatum_smear_result ,
max(if(o.concept_id=12,o.value_coded,null)) as chest_xray_result ,
max(if(o.concept_id=162202,o.value_coded,null)) as genexpert_result ,
max(if(o.concept_id=1272,o.value_coded,null)) as referral ,
max(if(o.concept_id=163752,o.value_coded,null)) as clinical_tb_diagnosis ,
max(if(o.concept_id=163414,o.value_coded,null)) as contact_invitation ,
max(if(o.concept_id=162275,o.value_coded,null)) as evaluated_for_ipt ,
max(if(o.concept_id=160557,o.value_coded,null)) as has_known_allergies ,
max(if(o.concept_id=162747,o.value_coded,null)) as has_chronic_illnesses_cormobidities ,
max(if(o.concept_id=121764,o.value_coded,null)) as has_adverse_drug_reaction ,
max(if(o.concept_id=5272,o.value_coded,null)) as pregnancy_status,
max(if(o.concept_id=164933,o.value_coded,null)) as wants_pregnancy,
max(if(o.concept_id=161033,o.value_coded,null)) as pregnancy_outcome,
max(if(o.concept_id=161655,o.value_numeric,null)) as anc_number,
max(if(o.concept_id=5596,date(o.value_datetime),null)) as expected_delivery_date,
max(if(o.concept_id=1427,date(o.value_datetime),null)) as last_menstrual_period,
max(if(o.concept_id=5624,o.value_numeric,null)) as gravida,
max(if(o.concept_id=1053,o.value_numeric,null)) as parity ,
max(if(o.concept_id=160080,o.value_numeric,null)) as full_term_pregnancies,
max(if(o.concept_id=1823,o.value_numeric,null)) as abortion_miscarriages ,
max(if(o.concept_id=160653,o.value_coded,null)) as family_planning_status,
max(if(o.concept_id=374,o.value_coded,null)) as family_planning_method,
max(if(o.concept_id=160575,o.value_coded,null)) as reason_not_using_family_planning ,
max(if(o.concept_id=1659,o.value_coded,null)) as tb_status,
max(if(o.concept_id=161654,o.value_text,null)) as tb_treatment_no,
max(if(o.concept_id=161652,o.value_coded,null)) as ctx_adherence,
max(if(o.concept_id=162229 or (o.concept_id=1282 and o.value_coded = 105281),o.value_coded,null)) as ctx_dispensed,
max(if(o.concept_id=164941,o.value_coded,null)) as dapsone_adherence,
max(if(o.concept_id=164940 or (o.concept_id=1282 and o.value_coded = 74250),o.value_coded,null)) as dapsone_dispensed,
max(if(o.concept_id=162230,o.value_coded,null)) as inh_dispensed,
max(if(o.concept_id=1658,o.value_coded,null)) as arv_adherence,
max(if(o.concept_id=160582,o.value_coded,null)) as poor_arv_adherence_reason,
max(if(o.concept_id=160632,o.value_text,null)) as poor_arv_adherence_reason_other,
max(if(o.concept_id=159423,o.value_coded,null)) as pwp_disclosure,
max(if(o.concept_id=161557,o.value_coded,null)) as pwp_partner_tested,
max(if(o.concept_id=159777,o.value_coded,null)) as condom_provided ,
max(if(o.concept_id=161558,o.value_coded,null)) as screened_for_sti,
max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
max(if(o.concept_id=164935,o.value_coded,null)) as sti_partner_notification,
max(if(o.concept_id=160581,o.value_coded,null)) as at_risk_population,
max(if(o.concept_id=159615,o.value_coded,null)) as system_review_finding,
max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date,
max(if(o.concept_id=160288,o.value_coded,null)) as next_appointment_reason,
max(if(o.concept_id=164947,o.value_coded,null)) as differentiated_care,
e.voided as voided
from encounter e 
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where uuid in('a0034eee-1940-4e35-847f-97537a35d05e','d1059fb9-a079-4feb-a749-eedd709ae542', '465a92f2-baf8-42e9-9612-53064be868e8')
) et on et.encounter_type_id=e.encounter_type
left outer join obs o on o.encounter_id=e.encounter_id 
	and o.concept_id in (1282,1246,161643,5089,5085,5086,5090,5088,5087,5242,5092,1343,5356,5272,161033,161655,5596,1427,5624,1053,160653,374,160575,1659,161654,161652,162229,162230,1658,160582,160632,159423,161557,159777,161558,160581,5096,163300, 164930, 160581, 1154, 160430, 164948, 164949, 164950, 1271, 307, 12, 162202, 1272, 163752, 163414, 162275, 160557, 162747,
121764, 164933, 160080, 1823, 164940, 164934, 164935, 159615, 160288, 164947)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date
;
SELECT "Completed processing HIV Followup data ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_laboratory_extract  uuid:  --------------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_laboratory_extract$$
CREATE PROCEDURE sp_populate_etl_laboratory_extract()
BEGIN
SELECT "Processing Laboratory data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_laboratory_extract(
uuid,
encounter_id,
patient_id,
location_id,
visit_date,
visit_id,
lab_test,
test_result,
-- date_test_requested,
-- date_test_result_received,
-- test_requested_by,
date_created,
created_by 
)
select 
o.uuid,
e.encounter_id,
e.patient_id,
e.location_id,
e.encounter_datetime as visit_date,
e.visit_id,
o.concept_id,
(CASE when o.concept_id in(5497,730,654,790,856,21) then o.value_numeric
	when o.concept_id in(299,1030,302,32, 1305) then o.value_coded
	END) AS test_result,
-- date requested,
-- date result received
-- test requested by
e.date_created,
e.creator
from encounter e 
inner join obs o on e.encounter_id=o.encounter_id and o.voided=0
and o.concept_id in (5497,730,299,654,790,856,1030,21,302,32, 1305) -- (5497-N,730-N,299-C,654-N,790-N,856-N,1030-C,21-N,302-C,32-C)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where uuid in('17a381d1-7e29-406a-b782-aa903b963c28', 'a0034eee-1940-4e35-847f-97537a35d05e')
) et on et.encounter_type_id=e.encounter_type
; 

/*-- >>>>>>>>>>>>>>> -----------------------------------  Wagners input ------------------------------------------------------------
insert into kenyaemr_etl.etl_laboratory_extract(
encounter_id,
patient_id,
visit_date,
visit_id,
lab_test,
test_result,
-- date_test_requested,
-- date_test_result_received,
-- test_requested_by,
date_created,
created_by 
)
select 
e.encounter_id,
e.patient_id,
e.encounter_datetime as visit_date,
e.visit_id,
o.concept_id,
(CASE when o.concept_id in(5497,730,654,790,856,21) then o.value_numeric
when o.concept_id in(299,1030,302,32) then o.value_coded
END) AS test_result,
-- date requested,
-- date result received
-- test requested by
e.date_created,
e.creator
from encounter e, obs o, encounter_type et 
where e.encounter_id=o.encounter_id and o.voided=0
and o.concept_id in (5497,730,299,654,790,856,1030,21,302,32) and et.encounter_type_id=e.encounter_type
group by e.encounter_id;

-- --------<<<<<<<<<<<<<<<<<<<< ------------------------------------------------------------------------------------------------------
*/
SELECT "Completed processing Laboratory data ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_pharmacy_extract table--------------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_pharmacy_extract$$
CREATE PROCEDURE sp_populate_etl_pharmacy_extract()
BEGIN
SELECT "Processing Pharmacy data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_pharmacy_extract(
obs_group_id,
patient_id,
uuid,
visit_date,
visit_id,
encounter_id,
date_created,
encounter_name,
location_id,
drug,
drug_name,
is_arv,
is_ctx,
is_dapsone,
frequency,
duration,
duration_units,
voided,
date_voided,
dispensing_provider
)
select 
	o.obs_group_id obs_group_id,
	o.person_id,
	max(if(o.concept_id=1282, o.uuid, null)),
	date(o.obs_datetime) as enc_date,
	e.visit_id,
	o.encounter_id,
	e.date_created,
	et.name as enc_name,
	e.location_id,
	max(if(o.concept_id = 1282 and o.value_coded is not null,o.value_coded, null)) as drug_dispensed,
	max(if(o.concept_id = 1282, cn.name, 0)) as drug_name, -- arv:1085
	max(if(o.concept_id = 1282 and cs.concept_set=1085, 1, 0)) as arv_drug, -- arv:1085
	max(if(o.concept_id = 1282 and o.value_coded = 105281,1, 0)) as is_ctx,
	max(if(o.concept_id = 1282 and o.value_coded = 74250,1, 0)) as is_dapsone,
	max(if(o.concept_id = 1443, o.value_numeric, null)) as dose,
	max(if(o.concept_id = 159368, o.value_numeric, null)) as duration,
	max(if(o.concept_id = 1732 and o.value_coded=1072,'Days',if(o.concept_id=1732 and o.value_coded=1073,'Weeks',if(o.concept_id=1732 and o.value_coded=1074,'Months',null)))) as duration_units,
	o.voided,
	o.date_voided,
	e.creator
from obs o
left outer join encounter e on e.encounter_id = o.encounter_id and e.voided=0
left outer join encounter_type et on et.encounter_type_id = e.encounter_type
left outer join concept_name cn on o.value_coded = cn.concept_id and cn.locale='en' and cn.concept_name_type='FULLY_SPECIFIED' -- SHORT'
left outer join concept_set cs on o.value_coded = cs.concept_id 
where o.voided=0 and o.concept_id in(1282,1732,159368,1443,1444)  and e.voided=0
group by o.obs_group_id, o.person_id, encounter_id
having drug_dispensed is not null and obs_group_id is not null;

update kenyaemr_etl.etl_pharmacy_extract
	set duration_in_days = if(duration_units= 'Days', duration,if(duration_units='Weeks',duration * 7,if(duration_units='Months',duration * 31,null)))
	where (duration is not null or duration <> "") and (duration_units is not null or duration_units <> "");

SELECT "Completed processing Pharmacy data ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------ create table etl_patient_treatment_event----------------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_program_discontinuation$$
CREATE PROCEDURE sp_populate_etl_program_discontinuation()
BEGIN
SELECT "Processing Program (HIV, TB, MCH ...) discontinuations ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_patient_program_discontinuation(
patient_id,
uuid,
visit_id,
visit_date,
program_uuid,
program_name,
encounter_id,
discontinuation_reason,
date_died,
transfer_facility,
transfer_date
)
select 
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_datetime, -- trying to make us of index
et.uuid,
(case et.uuid
	when '2bdada65-4c72-4a48-8730-859890e25cee' then 'HIV'
	when 'd3e3d723-7458-4b4e-8998-408e8a551a84' then 'TB'
	when '01894f88-dc73-42d4-97a3-0929118403fb' then 'MCH Child HEI'
	when '5feee3f1-aa16-4513-8bd0-5d9b27ef1208' then 'MCH Child'
	when '7c426cfc-3b47-4481-b55f-89860c21c7de' then 'MCH Mother'
end) as program_name,
e.encounter_id,
max(if(o.concept_id=161555, o.value_coded, null)) as reason_discontinued,
max(if(o.concept_id=1543, o.value_datetime, null)) as date_died,
max(if(o.concept_id=159495, o.value_text, null)) as to_facility,
max(if(o.concept_id=160649, o.value_datetime, null)) as to_date
from encounter e
inner join obs o on o.encounter_id=e.encounter_id and o.voided=0 and o.concept_id in (161555,1543,159495,160649)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where 
	uuid in('2bdada65-4c72-4a48-8730-859890e25cee','d3e3d723-7458-4b4e-8998-408e8a551a84','5feee3f1-aa16-4513-8bd0-5d9b27ef1208','7c426cfc-3b47-4481-b55f-89860c21c7de','01894f88-dc73-42d4-97a3-0929118403fb')
) et on et.encounter_type_id=e.encounter_type
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing discontinuation data ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_mch_enrollment-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_enrollment$$
CREATE PROCEDURE sp_populate_etl_mch_enrollment()
BEGIN
SELECT "Processing MCH Enrollments ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_mch_enrollment(
patient_id,
uuid,
visit_id,
visit_date,
location_id,
encounter_id,
anc_number,
gravida,
parity,
parity_abortion,
lmp,
lmp_estimated,
edd_ultrasound,
blood_group,
serology,
tb_screening,
bs_for_mps,
hiv_status,
hiv_test_date,
partner_hiv_status,
partner_hiv_test_date,
urine_microscopy,
urinary_albumin,
glucose_measurement,
urine_ph,
urine_gravity,
urine_nitrite_test,
urine_leukocyte_esterace_test,
urinary_ketone,
urine_bile_salt_test,
urine_bile_pigment_test,
urine_colour,
urine_turbidity,
urine_dipstick_for_blood,
-- date_of_discontinuation,
discontinuation_reason
)
select
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
max(if(o.concept_id=161655,o.value_numeric,null)) as anc_number,
max(if(o.concept_id=5624,o.value_numeric,null)) as gravida,
max(if(o.concept_id=160080,o.value_numeric,null)) as parity,
max(if(o.concept_id=1823,o.value_numeric,null)) as parity_abortion,
max(if(o.concept_id=1427,o.value_datetime,null)) as lmp,
max(if(o.concept_id=162095,o.value_datetime,null)) as lmp_estimated,
max(if(o.concept_id=5596,o.value_datetime,null)) as edd_ultrasound,
max(if(o.concept_id=300,o.value_coded,null)) as blood_group,
max(if(o.concept_id=299,o.value_coded,null)) as serology,
max(if(o.concept_id=160108,o.value_coded,null)) as tb_screening,
max(if(o.concept_id=32,o.value_coded,null)) as bs_for_mps,
max(if(o.concept_id=159427,o.value_coded,null)) as hiv_status,
max(if(o.concept_id=160554,o.value_datetime,null)) as hiv_test_date,
max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
max(if(o.concept_id=160082,o.value_datetime,null)) as partner_hiv_test_date,
max(if(o.concept_id=56,o.value_text,null)) as urine_microscopy,
max(if(o.concept_id=1875,o.value_coded,null)) as urinary_albumin,
max(if(o.concept_id=159734,o.value_coded,null)) as glucose_measurement,
max(if(o.concept_id=161438,o.value_numeric,null)) as urine_ph,
max(if(o.concept_id=161439,o.value_numeric,null)) as urine_gravity,
max(if(o.concept_id=161440,o.value_coded,null)) as urine_nitrite_test,
max(if(o.concept_id=161441,o.value_coded,null)) as urine_leukocyte_esterace_test,
max(if(o.concept_id=161442,o.value_coded,null)) as urinary_ketone,
max(if(o.concept_id=161444,o.value_coded,null)) as urine_bile_salt_test,
max(if(o.concept_id=161443,o.value_coded,null)) as urine_bile_pigment_test,
max(if(o.concept_id=162106,o.value_coded,null)) as urine_colour,
max(if(o.concept_id=162101,o.value_coded,null)) as urine_turbidity,
max(if(o.concept_id=162096,o.value_coded,null)) as urine_dipstick_for_blood,
-- max(if(o.concept_id=161655,o.value_text,null)) as date_of_discontinuation,
max(if(o.concept_id=161555,o.value_coded,null)) as discontinuation_reason
from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(161655,5624,160080,1823,1427,162095,5596,300,299,160108,32,159427,160554,1436,160082,56,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,161555)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where 
	uuid in('3ee036d8-7c13-4393-b5d6-036f2fe45126')
) et on et.encounter_type_id=e.encounter_type
group by e.encounter_id;
SELECT "Completed processing MCH Enrollments ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_mch_antenatal_visit-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_antenatal_visit$$
CREATE PROCEDURE sp_populate_etl_mch_antenatal_visit()
BEGIN
SELECT "Processing MCH antenatal visits ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_mch_antenatal_visit(
patient_id,
uuid,
visit_id,
visit_date,
location_id,
encounter_id,
provider,
temperature,
pulse_rate,
systolic_bp,
diastolic_bp,
respiratory_rate,
oxygen_saturation,
weight,
height,
muac,
hemoglobin,
pallor,
maturity,
fundal_height,
fetal_presentation,
lie,
fetal_heart_rate,
fetal_movement,
who_stage,
cd4,
arv_status,
urine_microscopy,
urinary_albumin,
glucose_measurement,
urine_ph,
urine_gravity,
urine_nitrite_test,
urine_leukocyte_esterace_test,
urinary_ketone,
urine_bile_salt_test,
urine_bile_pigment_test,
urine_colour,
urine_turbidity,
urine_dipstick_for_blood
)
select
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
e.creator,
max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_bp,
max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_bp,
max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
max(if(o.concept_id=21,o.value_numeric,null)) as hemoglobin,
max(if(o.concept_id=5245,o.value_coded,null)) as pallor,
max(if(o.concept_id=1438,o.value_numeric,null)) as maturity,
max(if(o.concept_id=1439,o.value_numeric,null)) as fundal_height,
max(if(o.concept_id=160090,o.value_coded,null)) as fetal_presentation,
max(if(o.concept_id=162089,o.value_coded,null)) as lie,
max(if(o.concept_id=1440,o.value_numeric,null)) as fetal_heart_rate,
max(if(o.concept_id=162107,o.value_coded,null)) as fetal_movement,
max(if(o.concept_id=5356,o.value_coded,null)) as who_stage,
max(if(o.concept_id=5497,o.value_numeric,null)) as cd4,
max(if(o.concept_id=1147,o.value_coded,null)) as arv_status,
max(if(o.concept_id=56,o.value_text,null)) as urine_microscopy,
max(if(o.concept_id=1875,o.value_coded,null)) as urinary_albumin,
max(if(o.concept_id=159734,o.value_coded,null)) as glucose_measurement,
max(if(o.concept_id=161438,o.value_numeric,null)) as urine_ph,
max(if(o.concept_id=161439,o.value_numeric,null)) as urine_gravity,
max(if(o.concept_id=161440,o.value_coded,null)) as urine_nitrite_test,
max(if(o.concept_id=161441,o.value_coded,null)) as urine_leukocyte_esterace_test,
max(if(o.concept_id=161442,o.value_coded,null)) as urinary_ketone,
max(if(o.concept_id=161444,o.value_coded,null)) as urine_bile_salt_test,
max(if(o.concept_id=161443,o.value_coded,null)) as urine_bile_pigment_test,
max(if(o.concept_id=162106,o.value_coded,null)) as urine_colour,
max(if(o.concept_id=162101,o.value_coded,null)) as urine_turbidity,
max(if(o.concept_id=162096,o.value_coded,null)) as urine_dipstick_for_blood
from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(5088,5087,5085,5086,5242,5092,5089,5090,1343,21,5245,1438,1439,160090,162089,1440,162107,5356,5497,1147,56,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096)
inner join 
(
	select encounter_type, uuid,name from form where 
	uuid in('e8f98494-af35-4bb8-9fc7-c409c8fed843')
) f on f.encounter_type=e.encounter_type
group by e.encounter_id;
SELECT "Completed processing MCH antenatal visits ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_mch_postnatal_visit-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_postnatal_visit$$
CREATE PROCEDURE sp_populate_etl_mch_postnatal_visit()
BEGIN
SELECT "Processing MCH postnatal visits ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_mch_postnatal_visit(
patient_id,
uuid,
visit_id,
visit_date,
location_id,
encounter_id,
provider,
temperature,
pulse_rate,
systolic_bp,
diastolic_bp,
respiratory_rate,
oxygen_saturation,
weight,
height,
muac,
hemoglobin,
arv_status,
general_condition,
breast,
cs_scar,
gravid_uterus,
episiotomy,
lochia,
mother_hiv_status,
condition_of_baby,
baby_feeding_method,
umblical_cord,
baby_immunization_started,
family_planning_counseling,
uterus_examination,
uterus_cervix_examination,
vaginal_examination,
parametrial_examination,
external_genitalia_examination,
ovarian_examination,
pelvic_lymph_node_exam
)
select
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
e.creator,
max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_bp,
max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_bp,
max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
max(if(o.concept_id=21,o.value_numeric,null)) as hemoglobin,
max(if(o.concept_id=1147,o.value_coded,null)) as arv_status,
max(if(o.concept_id=160085,o.value_coded,null)) as general_condition,
max(if(o.concept_id=159780,o.value_coded,null)) as breast,
max(if(o.concept_id=162128,o.value_coded,null)) as cs_scar,
max(if(o.concept_id=162110,o.value_coded,null)) as gravid_uterus,
max(if(o.concept_id=159840,o.value_coded,null)) as episiotomy,
max(if(o.concept_id=159844,o.value_coded,null)) as lochia,
max(if(o.concept_id=1396,o.value_coded,null)) as mother_hiv_status,
max(if(o.concept_id=162134,o.value_coded,null)) as condition_of_baby,
max(if(o.concept_id=1151,o.value_coded,null)) as baby_feeding_method,
max(if(o.concept_id=162121,o.value_coded,null)) as umblical_cord,
max(if(o.concept_id=162127,o.value_coded,null)) as baby_immunization_started,
max(if(o.concept_id=1382,o.value_coded,null)) as family_planning_counseling,
max(if(o.concept_id=160967,o.value_text,null)) as uterus_examination,
max(if(o.concept_id=160968,o.value_text,null)) as uterus_cervix_examination,
max(if(o.concept_id=160969,o.value_text,null)) as vaginal_examination,
max(if(o.concept_id=160970,o.value_text,null)) as parametrial_examination,
max(if(o.concept_id=160971,o.value_text,null)) as external_genitalia_examination,
max(if(o.concept_id=160975,o.value_text,null)) as ovarian_examination,
max(if(o.concept_id=160972,o.value_text,null)) as pelvic_lymph_node_exam
from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(5088,5087,5085,5086,5242,5092,5089,5090,1343,21,1147,160085,159780,162128,162110,159840,159844,1396,162134,1151,162121,162127,1382,160967,160968,160969,160970,160971,160975,160972)
inner join 
(
	select encounter_type, uuid,name from form where 
	uuid in('72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7')
) f on f.encounter_type=e.encounter_type
group by e.encounter_id;
SELECT "Completed processing MCH postnatal visits ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_tb_enrollment-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_tb_enrollment$$
CREATE PROCEDURE sp_populate_etl_tb_enrollment()
BEGIN
SELECT "Processing TB Enrollments ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_tb_enrollment(
patient_id,
uuid,
provider,
visit_id,
visit_date,
location_id,
encounter_id,
date_treatment_started,
district,
-- district_registration_number,
referred_by,
referral_date,
date_transferred_in,
facility_transferred_from,
district_transferred_from,
date_first_enrolled_in_tb_care,
weight,
height,
treatment_supporter,
relation_to_patient,
treatment_supporter_address,
treatment_supporter_phone_contact,
disease_classification,
patient_classification,
pulmonary_smear_result,
has_extra_pulmonary_pleurial_effusion,
has_extra_pulmonary_milliary,
has_extra_pulmonary_lymph_node,
has_extra_pulmonary_menengitis,
has_extra_pulmonary_skeleton,
has_extra_pulmonary_abdominal
-- has_extra_pulmonary_other,
-- treatment_outcome,
-- treatment_outcome_date 
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
max(if(o.concept_id=1113,o.value_datetime,null)) as date_treatment_started,
max(if(o.concept_id=161564,o.value_text,null)) as district,
-- max(if(o.concept_id=5085,o.value_numeric,null)) as district_registration_number,
max(if(o.concept_id=160540,o.value_coded,null)) as referred_by,
max(if(o.concept_id=161561,o.value_datetime,null)) as referral_date,
max(if(o.concept_id=160534,o.value_datetime,null)) as date_transferred_in,
max(if(o.concept_id=160535,o.value_text,null)) as facility_transferred_from,
max(if(o.concept_id=161551,o.value_text,null)) as district_transferred_from,
max(if(o.concept_id=161552,o.value_datetime,null)) as date_first_enrolled_in_tb_care,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=160638,o.value_text,null)) as treatment_supporter,
max(if(o.concept_id=160640,o.value_coded,null)) as relation_to_patient,
max(if(o.concept_id=160641,o.value_text,null)) as treatment_supporter_address,
max(if(o.concept_id=160642,o.value_text,null)) as treatment_supporter_phone_contact,
max(if(o.concept_id=160040,o.value_coded,null)) as disease_classification,
max(if(o.concept_id=159871,o.value_coded,null)) as patient_classification,
max(if(o.concept_id=159982,o.value_coded,null)) as pulmonary_smear_result,
max(if(o.concept_id=161356 and o.value_coded=130059,o.value_coded,null)) as has_extra_pulmonary_pleurial_effusion,
max(if(o.concept_id=161356 and o.value_coded=115753,o.value_coded,null)) as has_extra_pulmonary_milliary,
max(if(o.concept_id=161356 and o.value_coded=111953,o.value_coded,null)) as has_extra_pulmonary_lymph_node,
max(if(o.concept_id=161356 and o.value_coded=111967,o.value_coded,null)) as has_extra_pulmonary_menengitis,
max(if(o.concept_id=161356 and o.value_coded=112116,o.value_coded,null)) as has_extra_pulmonary_skeleton,
max(if(o.concept_id=161356 and o.value_coded=1350,o.value_coded,null)) as has_extra_pulmonary_abdominal
-- max(if(o.concept_id=161356,o.value_coded,null)) as has_extra_pulmonary_other
-- max(if(o.concept_id=159786,o.value_coded,null)) as treatment_outcome,
-- max(if(o.concept_id=159787,o.value_coded,null)) as treatment_outcome_date

from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(160540,161561,160534,160535,161551,161552,5089,5090,160638,160640,160641,160642,160040,159871,159982,161356)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where 
	uuid in('9d8498a4-372d-4dc4-a809-513a2434621e')
) et on et.encounter_type_id=e.encounter_type
group by e.encounter_id;
SELECT "Completed processing TB Enrollments ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_tb_follow_up_visit-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_tb_follow_up_visit$$
CREATE PROCEDURE sp_populate_etl_tb_follow_up_visit()
BEGIN
SELECT "Processing TB Followup visits ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_tb_follow_up_visit(
patient_id,
uuid,
provider,
visit_id ,
visit_date ,
location_id,
encounter_id,
spatum_test,
spatum_result,
result_serial_number,
quantity ,
date_test_done,
bacterial_colonie_growth,
number_of_colonies,
resistant_s,
resistant_r,
resistant_inh,
resistant_e,
sensitive_s,
sensitive_r,
sensitive_inh,
sensitive_e,
test_date,
hiv_status,
next_appointment_date
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
max(if(o.concept_id=159961,o.value_coded,null)) as spatum_test,
max(if(o.concept_id=307,o.value_coded,null)) as spatum_result,
max(if(o.concept_id=159968,o.value_numeric,null)) as result_serial_number,
max(if(o.concept_id=160023,o.value_numeric,null)) as quantity,
max(if(o.concept_id=159964,o.value_datetime,null)) as date_test_done,
max(if(o.concept_id=159982,o.value_coded,null)) as bacterial_colonie_growth,
max(if(o.concept_id=159952,o.value_numeric,null)) as number_of_colonies,
max(if(o.concept_id=159956 and o.value_coded=84360,o.value_coded,null)) as resistant_s,
max(if(o.concept_id=159956 and o.value_coded=767,o.value_coded,null)) as resistant_r,
max(if(o.concept_id=159956 and o.value_coded=78280,o.value_coded,null)) as resistant_inh,
max(if(o.concept_id=159956 and o.value_coded=75948,o.value_coded,null)) as resistant_e,
max(if(o.concept_id=159958 and o.value_coded=84360,o.value_coded,null)) as sensitive_s,
max(if(o.concept_id=159958 and o.value_coded=767,o.value_coded,null)) as sensitive_r,
max(if(o.concept_id=159958 and o.value_coded=78280,o.value_coded,null)) as sensitive_inh,
max(if(o.concept_id=159958 and o.value_coded=75948,o.value_coded,null)) as sensitive_e,
max(if(o.concept_id=159964,o.value_datetime,null)) as test_date,
max(if(o.concept_id=1169,o.value_coded,null)) as hiv_status,
max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date
from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(159961,307,159968,160023,159964,159982,159952,159956,159958,159964,1169,5096)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where 
	uuid in('fbf0bfce-e9f4-45bb-935a-59195d8a0e35')
) et on et.encounter_type_id=e.encounter_type
group by e.encounter_id;
SELECT "Completed processing TB Followup visits ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_tb_screening-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_tb_screening$$
CREATE PROCEDURE sp_populate_etl_tb_screening()
BEGIN
SELECT "Processing TB Screening data ", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_tb_screening(
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
resulting_tb_status ,
tb_treatment_start_date ,
notes 
) 
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.encounter_id, e.location_id,
max(case o.concept_id when 1659 then o.value_coded else "" end) as resulting_tb_status,
max(case o.concept_id when 1113 then date(o.value_datetime)  else "" end) as tb_treatment_start_date,
max(case o.concept_id when 160632 then value_text else "" end) as notes
from encounter e 
inner join form f on f.form_id=e.form_id and f.uuid in ("22c68f86-bbf0-49ba-b2d1-23fa7ccf0259", "59ed8e62-7f1f-40ae-a2e3-eabe350277ce")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (1659, 1113, 160632)
where e.voided=0
group by e.encounter_id;

SELECT "Completed processing TB Screening data ", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_hei_enrollment-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_hei_enrolment$$
CREATE PROCEDURE sp_populate_etl_hei_enrolment()
BEGIN
SELECT "Processing HEI Enrollments", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_hei_enrollment(
patient_id,
uuid,
provider,
visit_id,
visit_date,
location_id,
encounter_id,
child_exposed,
-- hei_id_number,
spd_number,
birth_weight,
gestation_at_birth,
date_first_seen,
birth_notification_number,
birth_certificate_number,
need_for_special_care,
reason_for_special_care,
referral_source ,
transfer_in,
transfer_in_date,
facility_transferred_from,
district_transferred_from,
date_first_enrolled_in_hei_care,
-- arv_prophylaxis,
mother_breastfeeding,
-- mother_on_NVP_during_breastfeeding,
TB_contact_history_in_household,
-- infant_mother_link,
mother_alive,
mother_on_pmtct_drugs,
mother_on_drug,
mother_on_art_at_infant_enrollment,
mother_drug_regimen,
parent_ccc_number,
mode_of_delivery,
place_of_delivery
-- exit_date,
-- exit_reason,
-- hiv_status_at_exit
)
select 
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
max(if(o.concept_id=5303,o.value_coded,null)) as child_exposed,
-- max(if(o.concept_id=5087,o.value_numeric,null)) as hei_id_number,
max(if(o.concept_id=162054,o.value_text,null)) as spd_number,
max(if(o.concept_id=5916,o.value_numeric,null)) as birth_weight,
max(if(o.concept_id=1409,o.value_text,null)) as gestation_at_birth,
max(if(o.concept_id=162140,o.value_datetime,null)) as date_first_seen,
max(if(o.concept_id=162051,o.value_text,null)) as birth_notification_number,
max(if(o.concept_id=162052,o.value_text,null)) as birth_certificate_number,
max(if(o.concept_id=161630,o.value_coded,null)) as need_for_special_care,
max(if(o.concept_id=161601,o.value_coded,null)) as reason_for_special_care,
max(if(o.concept_id=160540,o.value_coded,null)) as referral_source,
max(if(o.concept_id=160563,o.value_coded,null)) as transfer_in,
max(if(o.concept_id=160534,o.value_datetime,null)) as transfer_in_date,
max(if(o.concept_id=160535,o.value_text,null)) as facility_transferred_from,
max(if(o.concept_id=161551,o.value_text,null)) as district_transferred_from,
max(if(o.concept_id=160555,o.value_datetime,null)) as date_first_enrolled_in_hei_care,
-- max(if(o.concept_id=1282,o.value_coded,null)) as arv_prophylaxis,
max(if(o.concept_id=159941,o.value_coded,null)) as mother_breastfeeding,
-- max(if(o.concept_id=1282,o.value_coded,null)) as mother_on_NVP_during_breastfeeding,
max(if(o.concept_id=152460,o.value_coded,null)) as TB_contact_history_in_household,
-- max(if(o.concept_id=162121,o.value_coded,null)) as infant_mother_link,
max(if(o.concept_id=160429,o.value_coded,null)) as mother_alive,
max(if(o.concept_id=1148,o.value_coded,null)) as mother_on_pmtct_drugs,
max(if(o.concept_id=1086,o.value_coded,null)) as mother_on_drug,
max(if(o.concept_id=162055,o.value_coded,null)) as mother_on_art_at_infant_enrollment,
max(if(o.concept_id=1088,o.value_coded,null)) as mother_drug_regimen,
max(if(o.concept_id=162053,o.value_text,null)) as parent_ccc_number,
max(if(o.concept_id=5630,o.value_coded,null)) as mode_of_delivery,
max(if(o.concept_id=1572,o.value_coded,null)) as place_of_delivery
-- max(if(o.concept_id=160972,o.value_coded,null)) as exit_date
-- max(if(o.concept_id=161555,o.value_coded,null)) as exit_reason,
-- max(if(o.concept_id=159427,o.value_coded,null)) as hiv_status_at_exit
from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(5303,162054,5916,1409,162140,162051,162052,161630,161601,160540,160563,160534,160535,161551,160555,1282,159941,1282,152460,160429,1148,1086,162055,1088,162053,5630,1572,161555,159427)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where 
	uuid in('415f5136-ca4a-49a8-8db3-f994187c3af6')
) et on et.encounter_type_id=e.encounter_type
group by e.encounter_id ;  
SELECT "Completed processing HEI Enrollments", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_hei_follow_up_visit-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_hei_follow_up$$
CREATE PROCEDURE sp_populate_etl_hei_follow_up()
BEGIN
SELECT "Processing HEI Followup visits", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_hei_follow_up_visit(
patient_id,
uuid,
provider,
visit_id,
visit_date,
location_id,
encounter_id,
weight,
height,
infant_feeding,
tb_assessment_outcome,
social_smile_milestone,
head_control_milestone,
response_to_sound_milestone,
hand_extension_milestone,
sitting_milestone,
walking_milestone,
standing_milestone,
talking_milestone,
review_of_systems_developmental,
-- dna_pcr_sample_date,
dna_pcr_contextual_status,
dna_pcr_result,
-- dna_pcr_dbs_sample_code,
-- dna_pcr_results_date,
-- first_antibody_sample_date,
first_antibody_result,
-- first_antibody_dbs_sample_code,
-- first_antibody_result_date,
-- final_antibody_sample_date,
final_antibody_result,
-- final_antibody_dbs_sample_code,
-- final_antibody_result_date,
tetracycline_ointment_given,
pupil_examination,
sight_examination,
squint,
deworming_drug,
dosage,
unit,
next_appointment_date
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=1151,o.value_coded,null)) as infant_feeding,
max(if(o.concept_id=1659,o.value_coded,null)) as tb_assessment_outcome,
max(if(o.concept_id=162069 and o.value_coded=162056,o.value_coded,null)) as social_smile_milestone,
max(if(o.concept_id=162069 and o.value_coded=162057,o.value_coded,null)) as head_control_milestone,
max(if(o.concept_id=162069 and o.value_coded=162058,o.value_coded,null)) as response_to_sound_milestone,
max(if(o.concept_id=162069 and o.value_coded=162059,o.value_coded,null)) as hand_extension_milestone,
max(if(o.concept_id=162069 and o.value_coded=162061,o.value_coded,null)) as sitting_milestone,
max(if(o.concept_id=162069 and o.value_coded=162063,o.value_coded,null)) as walking_milestone,
max(if(o.concept_id=162069 and o.value_coded=162062,o.value_coded,null)) as standing_milestone,
max(if(o.concept_id=162069 and o.value_coded=162060,o.value_coded,null)) as talking_milestone,
max(if(o.concept_id=1189,o.value_coded,null)) as review_of_systems_developmental,
-- max(if(o.concept_id=159951,o.value_datetime,null)) as dna_pcr_sample_date,
max(if(o.concept_id=162084,o.value_coded,null)) as dna_pcr_contextual_status,
max(if(o.concept_id=844,o.value_coded,null)) as dna_pcr_result,
-- max(if(o.concept_id=162086,o.value_text,null)) as dna_pcr_dbs_sample_code,
-- max(if(o.concept_id=160082,o.value_datetime,null)) as dna_pcr_results_date,
-- max(if(o.concept_id=159951,o.value_datetime,null)) as first_antibody_sample_date,
max(if(o.concept_id=1040,o.value_coded,null)) as first_antibody_result,
-- max(if(o.concept_id=162086,o.value_text,null)) as first_antibody_dbs_sample_code,
-- max(if(o.concept_id=160082,o.value_datetime,null)) as first_antibody_result_date,
-- max(if(o.concept_id=159951,o.value_datetime,null)) as final_antibody_sample_date,
max(if(o.concept_id=1326,o.value_coded,null)) as final_antibody_result,
-- max(if(o.concept_id=162086,o.value_text,null)) as final_antibody_dbs_sample_code,
-- max(if(o.concept_id=160082,o.value_datetime,null)) as final_antibody_result_date,
max(if(o.concept_id=162077,o.value_coded,null)) as tetracycline_ointment_given,
max(if(o.concept_id=162064,o.value_coded,null)) as pupil_examination,
max(if(o.concept_id=162067,o.value_coded,null)) as sight_examination,
max(if(o.concept_id=162066,o.value_coded,null)) as squint,
max(if(o.concept_id=1282,o.value_coded,null)) as deworming_drug,
max(if(o.concept_id=1443,o.value_numeric,null)) as dosage,
max(if(o.concept_id=1621,o.value_text,null)) as unit,
max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date
from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(844,5089,5090,1151,1659,5096,162069,162069,162069,162069,162069,162069,162069,162069,1189,159951,162084,1030,162086,160082,159951,1040,162086,160082,159951,1326,162086,160082,162077,162064,162067,162066,1282,1443,1621)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where 
	uuid in('bcc6da85-72f2-4291-b206-789b8186a021','c6d09e05-1f25-4164-8860-9f32c5a02df0')
) et on et.encounter_type_id=e.encounter_type
group by e.encounter_id ; 
SELECT "Completed processing HEI Followup visits", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------- populate etl_mchs_delivery-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_delivery$$
CREATE PROCEDURE sp_populate_etl_mch_delivery()
BEGIN
SELECT "Processing MCH Delivery visits", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_mchs_delivery(
patient_id,
uuid,
provider,
visit_id,
visit_date,
location_id,
encounter_id,
data_entry_date,
duration_of_pregnancy,
mode_of_delivery,
date_of_delivery,
blood_loss,
condition_of_mother ,
apgar_score_1min,
apgar_score_5min,
apgar_score_10min,
resuscitation_done,
place_of_delivery,
delivery_assistant,
counseling_on_infant_feeding ,
counseling_on_exclusive_breastfeeding,
counseling_on_infant_feeding_for_hiv_infected,
mother_decision
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_datetime,
e.location_id,
e.encounter_id,
e.date_created,
max(if(o.concept_id=1789,o.value_numeric,null)) as duration_of_pregnancy,
max(if(o.concept_id=5630,o.value_coded,null)) as mode_of_delivery,
max(if(o.concept_id=5599,o.value_datetime,null)) as date_of_delivery,
max(if(o.concept_id=162092,o.value_coded,null)) as blood_loss,
max(if(o.concept_id=162093,o.value_text,null)) as condition_of_mother,
max(if(o.concept_id=159603,o.value_numeric,null)) as apgar_score_1min,
max(if(o.concept_id=159604,o.value_numeric,null)) as apgar_score_5min,
max(if(o.concept_id=159605,o.value_numeric,null)) as apgar_score_10min,
max(if(o.concept_id=162131,o.value_coded,null)) as resuscitation_done,
max(if(o.concept_id=1572,o.value_coded,null)) as place_of_delivery,
max(if(o.concept_id=1573,o.value_coded,null)) as delivery_assistant,
max(if(o.concept_id=1379 and o.value_coded=161651,o.value_coded,null)) as counseling_on_infant_feeding,
max(if(o.concept_id=1379 and o.value_coded=161096,o.value_coded,null)) as counseling_on_exclusive_breastfeeding,
max(if(o.concept_id=1379 and o.value_coded=162091,o.value_coded,null)) as counseling_on_infant_feeding_for_hiv_infected,
max(if(o.concept_id=1151,o.value_coded,null)) as mother_decision
from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(1789,5630,5599,162092,162093,159603,159604,159605,162131,1572,1573,1379,1151)
inner join 
(
	select encounter_type, uuid,name from form where 
	uuid in('496c7cc3-0eea-4e84-a04c-2292949e2f7f')
) f on f.encounter_type=e.encounter_type
group by e.encounter_id ;
SELECT "Completed processing MCH Delivery visits", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

-- ------------------------------------------- drug event ---------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_drug_event$$
CREATE PROCEDURE sp_drug_event()
BEGIN
SELECT "Processing Drug Event Data", CONCAT("Time: ", NOW());
INSERT INTO kenyaemr_etl.etl_drug_event(
uuid,
patient_id,
date_started,
regimen,
regimen_name,
regimen_line,
discontinued,
regimen_discontinued,
date_discontinued,
reason_discontinued,
reason_discontinued_other,
voided
)
SELECT 
o.uuid,
o.patient_id, 
o.start_date,
group_concat(distinct cn.name order by o.order_id) as regimen,
(CASE 
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "ABC+3TC+LPV/r"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DIDANOSINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "ABC+ddI+LPV/r"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DARUNAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "AZT+3TC+DRV/r"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DARUNAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "ABC+3TC+DRV/r"
-- ---
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "AZT+3TC+LPV/r"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ATAZANAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "AZT+3TC+ATV/r"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "TDF+3TC+LPV/r"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "TDF+ABC+LPV/r"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ATAZANAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "TDF+3TC+ATV/r"
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "d4T+3TC+LPV/r"
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "d4T+ABC+LPV/r"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DIDANOSINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "AZT+ddI+LPV/r"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "TDF+AZT+LPV/r"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "AZT+ABC+LPV/r"
	
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("NEVIRAPINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "AZT+3TC+NVP"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("EFAVIRENZ", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "AZT+3TC+EFV"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "AZT+3TC+ABC"
	
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("NEVIRAPINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "TDF+3TC+NVP"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("EFAVIRENZ", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "TDF+3TC+EFV"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "TDF+3TC+ABC"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "TDF+3TC+AZT"
	
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("NEVIRAPINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "d4T+3TC+NVP"
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("EFAVIRENZ", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "d4T+3TC+EFV"
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "d4T+3TC+ABC"
	
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("NEVIRAPINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "ABC+3TC+NVP"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("EFAVIRENZ", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "ABC+3TC+EFV"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "ABC+3TC+AZT"

	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DOLUTEGRAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "AZT+3TC+DTG"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DOLUTEGRAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "TDF+3TC+DTG"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DOLUTEGRAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "ABC+3TC+DTG"
	
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ATAZANAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "TDF+3TC+ATV/r"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ATAZANAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "AZT+3TC+ATV/r"
	
END) as regimen_name,
(CASE 
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DIDANOSINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DARUNAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DARUNAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
-- ---
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ATAZANAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ATAZANAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DIDANOSINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LOPINAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("RITONAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 THEN "2nd Line"
	
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("NEVIRAPINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("EFAVIRENZ", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "2nd Line"
	
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("NEVIRAPINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("EFAVIRENZ", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "2nd Line"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "2nd Line"
	
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("NEVIRAPINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("EFAVIRENZ", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("STAVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "2nd Line"
	
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("NEVIRAPINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("EFAVIRENZ", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DOLUTEGRAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DOLUTEGRAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	WHEN FIND_IN_SET("ABACAVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("DOLUTEGRAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "1st Line"
	
	WHEN FIND_IN_SET("TENOFOVIR", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ATAZANAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "2nd Line"
	WHEN FIND_IN_SET("ZIDOVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("LAMIVUDINE", group_concat(distinct cn.name order by o.order_id)) > 0 AND FIND_IN_SET("ATAZANAVIR", group_concat(distinct cn.name order by o.order_id)) > 0  THEN "2nd Line"

END) as regimen_line,
-- cs.concept_set, 
d.discontinued,
d.drugs,
d.discontinued_date,
d.discontinued_reason,
d.discontinued_reason_non_coded,
o.voided 
from orders o
left outer join concept_name cn on o.concept_id = cn.concept_id and cn.locale='en' and cn.concept_name_type='FULLY_SPECIFIED' 
left outer join concept_set cs on o.concept_id = cs.concept_id 
left outer join (
SELECT 
o.patient_id, 
group_concat(distinct cn.name order by o.order_id) as drugs,
cs.concept_set, 
o.start_date, 
o.discontinued, 
o.discontinued_date, 
o.discontinued_reason,
discontinued_reason_non_coded 
from orders o
left outer join concept_name cn on o.concept_id = cn.concept_id and cn.locale='en' and cn.concept_name_type='FULLY_SPECIFIED' 
left outer join concept_set cs on o.concept_id = cs.concept_id 
where o.voided=0 and cs.concept_set = 1085 -- and o.discontinued=1 -- start and stopped dates should instead be used
group by o.discontinued_date

) d on d.patient_id = o.patient_id and d.start_date=o.start_date
where o.voided=0 and cs.concept_set = 1085
group by o.patient_id, o.start_date
;
SELECT "Completed processing Drug Event Data", CONCAT("Time: ", NOW());
END$$
DELIMITER ;


-- ------------------------------------ populate hts test table ----------------------------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_hts_test$$
CREATE PROCEDURE sp_populate_hts_test()
BEGIN 
SELECT "Processing hts tests";
INSERT INTO kenyaemr_etl.etl_hts_test (
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
remarks,
voided
)
select
e.patient_id,
e.visit_id,
e.encounter_id,
e.uuid,
e.location_id,
e.creator,
e.date_created,
e.encounter_datetime as visit_date,
max(if((o.concept_id=162084 and o.value_coded=162082 and f.uuid = "402dc5d7-46da-42d4-b2be-f43ea4ad87b0") or (f.uuid = "b08471f6-0892-4bf7-ab2b-bf79797b8ea4"), 2, 1)) as test_type , -- 2 for confirmation, 1 for initial
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
inner join form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (162084, 164930, 160581, 164401, 164951, 162558, 1710, 164959, 164956,
                                                                                 159427, 164848, 6096, 1659, 164952, 163042)
inner join (
             select
               o.person_id,
               o.encounter_id,
               o.obs_group_id,
               max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
               max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
               max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
               max(if(o.concept_id=164964,o.value_text,null)) as lot_no,
               max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
             from obs o 
             inner join encounter e on e.encounter_id = o.encounter_id
             inner join form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")   
             where o.concept_id in (1040, 1326, 164962, 164964, 162502)
             group by e.encounter_id, o.obs_group_id
           ) t on e.encounter_id = t.encounter_id
group by e.encounter_id;
SELECT "Completed processing hts tests";
END$$
DELIMITER ;

-- ------------------------------------ POPULATE HTS LINKAGES AND REFERRALS -------------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_hts_linkage_and_referral$$
CREATE PROCEDURE sp_populate_hts_linkage_and_referral()
BEGIN 
SELECT "Processing hts linkages, referrals and tracing";
INSERT INTO kenyaemr_etl.etl_hts_referral_and_linkage (
  patient_id,
  visit_id,
  encounter_id,
  encounter_uuid,
  encounter_location,
  creator,
  date_created,
  visit_date,
  tracing_type,
  tracing_status,
  facility_linked_to,
  ccc_number,
  provider_handed_to,
  voided
)
  select
    e.patient_id,
    e.visit_id,
    e.encounter_id,
    e.uuid,
    e.location_id,
    e.creator,
    e.date_created,
    e.encounter_datetime as visit_date,
    max(if(o.concept_id=164966,(case o.value_coded when 1650 then "Phone" when 164965 then "Physical" else "" end),null)) as tracing_type ,
    max(if(o.concept_id=159811,(case o.value_coded when 1065 then "Contacted and linked" when 1066 then "Contacted but not linked" else "" end),null)) as tracing_status,
    max(if(o.concept_id=162724,o.value_text,null)) as facility_linked_to,
    max(if(o.concept_id=162053,o.value_numeric,null)) as ccc_number,
    max(if(o.concept_id=1473,o.value_text,null)) as provider_handed_to,
    e.voided
  from encounter e
  inner join form f on f.form_id = e.form_id and f.uuid = "050a7f12-5c52-4cad-8834-863695af335d"
  left outer join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164966, 159811, 162724, 162053, 1473)
  group by e.encounter_id;

-- fetch locally enrolled clients who had went through HTS

  INSERT INTO kenyaemr_etl.etl_hts_referral_and_linkage (
  patient_id,
  visit_id,
  encounter_id,
  encounter_uuid,
  encounter_location,
  creator,
  date_created,
  visit_date,
  tracing_status,
  facility_linked_to,
  ccc_number,
  voided
)
select
    e.patient_id,
    e.visit_id,
    e.encounter_id,
    e.uuid,
    e.location_id,
    e.creator,
    e.date_created,
    e.encounter_datetime as visit_date,
    "Enrolled" as contact_status,
    (select name from location
        where location_id in (select property_value
        from global_property
        where property='kenyaemr.defaultLocation'))  as facility_linked_to,
    pi.identifier as ccc_number,
    e.voided
 from encounter e 
 inner join encounter_type et on e.encounter_type = et.encounter_type_id and et.uuid = "de78a6be-bfc5-4634-adc3-5f1a280455cc"
 inner join form f on f.form_id = e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
 left outer join patient_identifier pi on pi.patient_id = e.patient_id
 left join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id and pit.uuid = '05ee9cf4-7242-4a17-b4d4-00f707265c8a'
;

END$$
DELIMITER ;
-- ----------------------------------- UPDATE DASHBOARD TABLE ---------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_dashboard_table$$
CREATE PROCEDURE sp_update_dashboard_table()
BEGIN

DECLARE startDate DATE;
DECLARE endDate DATE;
DECLARE reportingPeriod VARCHAR(20);

SET startDate = DATE_FORMAT(NOW() - INTERVAL 1 MONTH, '%Y-%m-01');
SET endDate = DATE_FORMAT(LAST_DAY(NOW() - INTERVAL 1 MONTH), '%Y-%m-%d');
SET reportingPeriod = DATE_FORMAT(NOW() - INTERVAL 1 MONTH, '%Y-%M');

-- CURRENT IN CARE 
DROP TABLE IF EXISTS kenyaemr_etl.etl_current_in_care;

CREATE TABLE kenyaemr_etl.etl_current_in_care AS
select fup.visit_date,fup.patient_id,p.dob,p.Gender, min(e.visit_date) as enroll_date,
max(fup.visit_date) as latest_vis_date,
mid(max(concat(fup.visit_date,fup.next_appointment_date)),11) as latest_tca,
p.unique_patient_no,
max(d.visit_date) as date_discontinued,
d.patient_id as disc_patient,
de.patient_id as started_on_drugs
from kenyaemr_etl.etl_patient_hiv_followup fup
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=fup.patient_id
join kenyaemr_etl.etl_hiv_enrollment e on fup.patient_id=e.patient_id
left outer join kenyaemr_etl.etl_drug_event de on e.patient_id = de.patient_id and date(date_started) <= endDate
left outer JOIN
(select patient_id, visit_date from kenyaemr_etl.etl_patient_program_discontinuation
where date(visit_date) <= endDate and program_name='HIV'
group by patient_id
) d on d.patient_id = fup.patient_id
where fup.visit_date <= endDate
group by patient_id
having (
(date(latest_tca) > endDate and (date(latest_tca) > date(date_discontinued) or disc_patient is null )) or
(((date(latest_tca) between startDate and endDate) and (date(latest_vis_date) >= date(latest_tca))) and (date(latest_tca) > date(date_discontinued) or disc_patient is null )) )
;

-- ADD INDICES
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(enroll_date);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(latest_vis_date);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(latest_tca);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(started_on_drugs);


DROP TABLE IF EXISTS kenyaemr_etl.etl_last_month_newly_enrolled_in_care;
CREATE TABLE kenyaemr_etl.etl_last_month_newly_enrolled_in_care (
patient_id INT(11) not null
);

INSERT INTO kenyaemr_etl.etl_last_month_newly_enrolled_in_care
select distinct e.patient_id 
from kenyaemr_etl.etl_hiv_enrollment e 
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=e.patient_id 
where  e.entry_point <> 160563  and transfer_in_date is null 
and date(e.visit_date) between startDate and endDate and (e.patient_type not in (160563, 164931, 159833) or e.patient_type is null or e.patient_type='');


DROP TABLE IF EXISTS kenyaemr_etl.etl_last_month_newly_on_art;
CREATE TABLE kenyaemr_etl.etl_last_month_newly_on_art (
patient_id INT(11) not null
);

INSERT INTO kenyaemr_etl.etl_last_month_newly_on_art
select distinct net.patient_id 
from ( 
select e.patient_id,e.date_started, 
e.gender,
e.dob,
d.visit_date as dis_date, 
if(d.visit_date is not null, 1, 0) as TOut,
e.regimen, e.regimen_line, e.alternative_regimen, 
mid(max(concat(fup.visit_date,fup.next_appointment_date)),11) as latest_tca, 
max(if(enr.date_started_art_at_transferring_facility is not null and enr.facility_transferred_from is not null, 1, 0)) as TI_on_art,
max(if(enr.transfer_in_date is not null, 1, 0)) as TIn, 
max(fup.visit_date) as latest_vis_date
from (select e.patient_id,p.dob,p.Gender,min(e.date_started) as date_started, 
mid(min(concat(e.date_started,e.regimen_name)),11) as regimen, 
mid(min(concat(e.date_started,e.regimen_line)),11) as regimen_line, 
max(if(discontinued,1,0))as alternative_regimen 
from kenyaemr_etl.etl_drug_event e 
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=e.patient_id 
group by e.patient_id) e 
left outer join kenyaemr_etl.etl_patient_program_discontinuation d on d.patient_id=e.patient_id 
left outer join kenyaemr_etl.etl_hiv_enrollment enr on enr.patient_id=e.patient_id 
left outer join kenyaemr_etl.etl_patient_hiv_followup fup on fup.patient_id=e.patient_id 
where  date(e.date_started) between startDate and endDate 
group by e.patient_id 
having TI_on_art=0
)net;

-- populate people booked today
TRUNCATE TABLE kenyaemr_etl.etl_patients_booked_today;
ALTER TABLE kenyaemr_etl.etl_patients_booked_today AUTO_INCREMENT = 1;

INSERT INTO kenyaemr_etl.etl_patients_booked_today(patient_id, last_visit_date)
SELECT patient_id, max(visit_date) 
FROM kenyaemr_etl.etl_patient_hiv_followup
WHERE date(next_appointment_date) = CURDATE()
GROUP BY patient_id;

END$$
DELIMITER ;

-- ------------- populate etl_ipt_screening-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_screening$$
CREATE PROCEDURE sp_populate_etl_ipt_screening()
BEGIN
SELECT "Processing IPT screening forms", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_ipt_screening(
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
ipt_started
)
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.encounter_id, e.location_id,
max(o.value_coded) as ipt_started
from encounter e 
inner join form f on f.form_id=e.form_id and f.uuid in ("22c68f86-bbf0-49ba-b2d1-23fa7ccf0259", "59ed8e62-7f1f-40ae-a2e3-eabe350277ce")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id=1265
where e.voided=0
group by e.encounter_id;

SELECT "Completed processing IPT screening forms", CONCAT("Time: ", NOW());
END$$
DELIMITER ;


-- ------------- populate etl_ipt_followup-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_follow_up$$
CREATE PROCEDURE sp_populate_etl_ipt_follow_up()
BEGIN
SELECT "Processing IPT followup forms", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_ipt_follow_up(
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
ipt_due_date,
date_collected_ipt,
hepatotoxity,
peripheral_neuropathy,
rash,
adherence,
outcome,
discontinuation_reason,
action_taken
)
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_datetime, e.encounter_id, e.location_id,
max(if(o.concept_id = 164073, o.value_datetime, "" )) as ipt_due_date,
max(if(o.concept_id = 164074, o.value_datetime, "" )) as date_collected_ipt,
max(if(o.concept_id = 159098, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as hepatotoxity,
max(if(o.concept_id = 118983, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as peripheral_neuropathy,
max(if(o.concept_id = 512, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as rash,
max(if(o.concept_id = 164075, (case o.value_coded when 159407 then "Poor" when 159405 then "Good" when 159406 then "Fair" when 164077 then "Very Good" when 164076 then "Excellent" when 1067 then "Unknown" else "" end), "" )) as adherence,
max(if(o.concept_id = 160433, (case o.value_coded when 1267 then "Completed" when 5240 then "Lost to followup" when 159836 then "Discontinued" when 160034 then "Died" when 159492 then "Transferred Out" else "" end), "" )) as outcome,
max(if(o.concept_id = 1266, (case o.value_coded when 102 then "Drug Toxicity" when 112141 then "TB" when 5622 then "Other" else "" end), "" )) as discontinuation_reason,
max(if(o.concept_id = 160632, o.value_text, "" )) as action_taken
from obs o 
inner join encounter e on e.encounter_id = o.encounter_id and e.voided =0 
inner join form f on f.form_id=e.form_id and f.uuid in ("22c68f86-bbf0-49ba-b2d1-23fa7ccf0259")
where o.concept_id in (164073, 164074, 159098, 118983, 512, 164075, 160433, 1266, 160632)
group by e.encounter_id ;
SELECT "Completed processing IPT followup forms", CONCAT("Time: ", NOW());
END$$
DELIMITER ;


-- ------------------------------------------- running all procedures -----------------------------


DELIMITER $$
DROP PROCEDURE IF EXISTS sp_first_time_setup$$
CREATE PROCEDURE sp_first_time_setup()
BEGIN
DECLARE populate_script_id INT(11);
SELECT "Beginning first time setup", CONCAT("Time: ", NOW());
INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('initial_population_of_tables', NOW());
SET populate_script_id = LAST_INSERT_ID();

CALL sp_populate_etl_patient_demographics();
CALL sp_populate_etl_hiv_enrollment();
CALL sp_populate_etl_hiv_followup();
CALL sp_populate_etl_laboratory_extract();
CALL sp_populate_etl_pharmacy_extract();
CALL sp_populate_etl_program_discontinuation();
CALL sp_populate_etl_mch_enrollment();
CALL sp_populate_etl_mch_antenatal_visit();
CALL sp_populate_etl_mch_postnatal_visit();
CALL sp_populate_etl_tb_enrollment();
CALL sp_populate_etl_tb_follow_up_visit();
CALL sp_populate_etl_tb_screening();
CALL sp_populate_etl_hei_enrolment();
CALL sp_populate_etl_hei_follow_up();
CALL sp_populate_etl_mch_delivery();
CALL sp_drug_event();
CALL sp_populate_hts_test();
CALL sp_populate_hts_linkage_and_referral();
CALL sp_populate_etl_ipt_screening();
CALL sp_populate_etl_ipt_follow_up();
CALL sp_update_dashboard_table();

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed first time setup", CONCAT("Time: ", NOW());
END$$
DELIMITER ;

CALL create_etl_tables();
CALL sp_first_time_setup();



