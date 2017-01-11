DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_patient_demographics$$
CREATE PROCEDURE sp_update_etl_patient_demographics()
BEGIN
-- update etl_patient_demographics table
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
inner join person_name pn on pn.person_id = p.person_id and pn.voided=0
where pn.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or pn.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or pn.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or p.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or p.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or p.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
GROUP BY p.person_id
) p
ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name=p.middle_name, family_name=p.family_name, DOB=p.birthdate, dead=p.dead, voided=p.voided, death_date=p.death_date;


-- update etl_patient_demographics with patient attributes: birthplace, citizenship, mother_name, phone number and kin's details
update kenyaemr_etl.etl_patient_demographics d 
inner join 
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
where pa.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or pa.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or pa.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
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


END$$
DELIMITER ;


DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_hiv_enrollment$$
CREATE PROCEDURE sp_update_etl_hiv_enrollment()
BEGIN
-- update patient_hiv_enrollment table
-- uuid: de78a6be-bfc5-4634-adc3-5f1a280455cc

insert into kenyaemr_etl.etl_hiv_enrollment (
patient_id,
uuid,
visit_id,
visit_date,
encounter_id,
encounter_provider,
date_created,
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
e.encounter_datetime,
e.encounter_id,
e.creator,
e.date_created,
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
where e.voided=0 and e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.patient_id, e.encounter_id
order by e.patient_id
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date),encounter_provider=VALUES(encounter_provider),date_first_enrolled_in_care=VALUES(date_first_enrolled_in_care),entry_point=VALUES(entry_point),transfer_in_date=VALUES(transfer_in_date),
facility_transferred_from=VALUES(facility_transferred_from),district_transferred_from=VALUES(district_transferred_from),date_started_art_at_transferring_facility=VALUES(date_started_art_at_transferring_facility),date_confirmed_hiv_positive=VALUES(date_confirmed_hiv_positive),facility_confirmed_hiv_positive=VALUES(facility_confirmed_hiv_positive),
arv_status=VALUES(arv_status),name_of_treatment_supporter=VALUES(name_of_treatment_supporter),relationship_of_treatment_supporter=VALUES(relationship_of_treatment_supporter),treatment_supporter_telephone=VALUES(treatment_supporter_telephone),treatment_supporter_address=VALUES(treatment_supporter_address),voided=VALUES(voided) 
;

END$$
DELIMITER ;

-- ------------- update etl_hiv_followup--------------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_hiv_followup$$
CREATE PROCEDURE sp_update_etl_hiv_followup()
BEGIN

INSERT INTO kenyaemr_etl.etl_patient_hiv_followup(
patient_id,
visit_id,
visit_date,
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
who_stage,
pregnancy_status,
pregnancy_outcome,
anc_number,
expected_delivery_date,
last_menstrual_period,
gravida,
parity,
family_planning_status,
family_planning_method,
reason_not_using_family_planning,
tb_status,
tb_treatment_no,
ctx_adherence,
ctx_dispensed,
inh_dispensed,
arv_adherence,
poor_arv_adherence_reason,
poor_arv_adherence_reason_other,
pwp_disclosure,
pwp_partner_tested,
condom_provided,
screened_for_sti,
at_risk_population,
next_appointment_date,
voided
)
select 
e.patient_id,
e.visit_id,
date(e.encounter_datetime) as visit_date,
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
max(if(o.concept_id=5356,o.value_coded,null)) as who_stage ,
max(if(o.concept_id=5272,o.value_coded,null)) as pregnancy_status,
max(if(o.concept_id=161033,o.value_coded,null)) as pregnancy_outcome,
max(if(o.concept_id=161655,o.value_numeric,null)) as anc_number,
max(if(o.concept_id=5596,date(o.value_datetime),null)) as expected_delivery_date,
max(if(o.concept_id=1427,date(o.value_datetime),null)) as last_menstrual_period,
max(if(o.concept_id=5624,o.value_numeric,null)) as gravida,
max(if(o.concept_id=1053,o.value_numeric,null)) as parity ,
max(if(o.concept_id=160653,o.value_coded,null)) as family_planning_status,
max(if(o.concept_id=374,o.value_coded,null)) as family_planning_method,
max(if(o.concept_id=160575,o.value_coded,null)) as reason_not_using_family_planning ,
max(if(o.concept_id=1659,o.value_coded,null)) as tb_status,
max(if(o.concept_id=161654,o.value_text,null)) as tb_treatment_no,
max(if(o.concept_id=161652,o.value_coded,null)) as ctx_adherence,
max(if(o.concept_id=162229,o.value_coded,null)) as ctx_dispensed,
max(if(o.concept_id=162230,o.value_coded,null)) as inh_dispensed,
max(if(o.concept_id=1658,o.value_coded,null)) as arv_adherence,
max(if(o.concept_id=160582,o.value_coded,null)) as poor_arv_adherence_reason,
max(if(o.concept_id=160632,o.value_text,null)) as poor_arv_adherence_reason_other,
max(if(o.concept_id=159423,o.value_coded,null)) as pwp_disclosure,
max(if(o.concept_id=161557,o.value_coded,null)) as pwp_partner_tested,
max(if(o.concept_id=159777,o.value_coded,null)) as condom_provided ,
max(if(o.concept_id=161558,o.value_coded,null)) as screened_for_sti,
max(if(o.concept_id=160581,o.value_coded,null)) as at_risk_population,
max(if(o.concept_id=5096,o.value_datetime,null)) as next_appointment_date,
e.voided as voided
from encounter e 
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where uuid in('a0034eee-1940-4e35-847f-97537a35d05e','d1059fb9-a079-4feb-a749-eedd709ae542', '465a92f2-baf8-42e9-9612-53064be868e8')
) et on et.encounter_type_id=e.encounter_type
left outer join obs o on o.encounter_id=e.encounter_id 
	and o.concept_id in (1246,161643,5089,5085,5086,5090,5088,5087,5242,5092,1343,5356,5272,161033,161655,5596,1427,5624,1053,160653,374,160575,1659,161654,161652,162229,162230,1658,160582,160632,159423,161557,159777,161558,160581,5096)
where e.voided=0 and e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.patient_id, visit_date
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date),encounter_provider=VALUES(encounter_provider),visit_scheduled=VALUES(visit_scheduled),
person_present=VALUES(person_present),weight=VALUES(weight),systolic_pressure=VALUES(systolic_pressure),diastolic_pressure=VALUES(diastolic_pressure),height=VALUES(height),temperature=VALUES(temperature),pulse_rate=VALUES(pulse_rate),respiratory_rate=VALUES(respiratory_rate),
oxygen_saturation=VALUES(oxygen_saturation),muac=VALUES(muac),who_stage=VALUES(who_stage),pregnancy_status=VALUES(pregnancy_status),pregnancy_outcome=VALUES(pregnancy_outcome),anc_number=VALUES(anc_number),expected_delivery_date=VALUES(expected_delivery_date),
last_menstrual_period=VALUES(last_menstrual_period),gravida=VALUES(gravida),parity=VALUES(parity),family_planning_status=VALUES(family_planning_status),family_planning_method=VALUES(family_planning_method),reason_not_using_family_planning=VALUES(reason_not_using_family_planning),
tb_status=VALUES(tb_status),tb_treatment_no=VALUES(tb_treatment_no),ctx_adherence=VALUES(ctx_adherence),ctx_dispensed=VALUES(ctx_dispensed),inh_dispensed=VALUES(inh_dispensed),arv_adherence=VALUES(arv_adherence),poor_arv_adherence_reason=VALUES(poor_arv_adherence_reason),
poor_arv_adherence_reason_other=VALUES(poor_arv_adherence_reason_other),pwp_disclosure=VALUES(pwp_disclosure),pwp_partner_tested=VALUES(pwp_partner_tested),condom_provided=VALUES(condom_provided),screened_for_sti=VALUES(screened_for_sti),at_risk_population=VALUES(at_risk_population),
next_appointment_date=VALUES(next_appointment_date),voided=VALUES(voided)
;

END$$
DELIMITER ;


-- ------------ create table etl_patient_treatment_event----------------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_program_discontinuation$$
CREATE PROCEDURE sp_update_etl_program_discontinuation()
BEGIN
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
e.encounter_datetime,
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
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date),discontinuation_reason=VALUES(discontinuation_reason),
date_died=VALUES(date_died),transfer_facility=VALUES(transfer_facility),transfer_date=VALUES(transfer_date)
;

END$$
DELIMITER ;

-- ------------- update etl_mch_enrollment------------------------- TO BE CHECKED AGAIN
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_mch_enrollment$$
CREATE PROCEDURE sp_update_etl_mch_enrollment()
BEGIN
insert into kenyaemr_etl.etl_mch_enrollment(
patient_id,
uuid,
visit_id,
visit_date,
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
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date),anc_number=VALUES(anc_number),gravida=VALUES(gravida),parity=VALUES(parity),parity_abortion=VALUES(parity_abortion),lmp=VALUES(lmp),lmp_estimated=VALUES(lmp_estimated),
edd_ultrasound=VALUES(edd_ultrasound),blood_group=VALUES(blood_group),serology=VALUES(serology),tb_screening=VALUES(tb_screening),bs_for_mps=VALUES(bs_for_mps),hiv_status=VALUES(hiv_status),hiv_test_date=VALUES(hiv_status),partner_hiv_status=VALUES(partner_hiv_status),partner_hiv_test_date=VALUES(partner_hiv_test_date),
urine_microscopy=VALUES(urine_microscopy),urinary_albumin=VALUES(urinary_albumin),glucose_measurement=VALUES(glucose_measurement),urine_ph=VALUES(urine_ph),urine_gravity=VALUES(urine_gravity),urine_nitrite_test=VALUES(urine_nitrite_test),urine_leukocyte_esterace_test=VALUES(urine_leukocyte_esterace_test),urinary_ketone=VALUES(urinary_ketone),
urine_bile_salt_test=VALUES(urine_bile_salt_test),urine_bile_pigment_test=VALUES(urine_bile_pigment_test),urine_colour=VALUES(urine_colour),urine_turbidity=VALUES(urine_turbidity),urine_dipstick_for_blood=VALUES(urine_dipstick_for_blood),discontinuation_reason=VALUES(discontinuation_reason)
;

END$$
DELIMITER ;

-- ------------- update etl_mch_antenatal_visit-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_mch_antenatal_visit$$
CREATE PROCEDURE sp_update_etl_mch_antenatal_visit()
BEGIN

insert into kenyaemr_etl.etl_mch_antenatal_visit(
patient_id,
uuid,
visit_id,
visit_date,
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
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date),provider=VALUES(provider),temperature=VALUES(temperature),pulse_rate=VALUES(pulse_rate),systolic_bp=VALUES(systolic_bp),diastolic_bp=VALUES(diastolic_bp),respiratory_rate=VALUES(respiratory_rate),oxygen_saturation=VALUES(oxygen_saturation),
weight=VALUES(weight),height=VALUES(height),muac=VALUES(muac),hemoglobin=VALUES(hemoglobin),pallor=VALUES(pallor),maturity=VALUES(maturity),fundal_height=VALUES(fundal_height),fetal_presentation=VALUES(fetal_presentation),lie=VALUES(lie),fetal_heart_rate=VALUES(fetal_heart_rate),fetal_movement=VALUES(fetal_movement),who_stage=VALUES(who_stage),cd4=VALUES(cd4),arv_status=VALUES(arv_status),
urine_microscopy=VALUES(urine_microscopy),urinary_albumin=VALUES(urinary_albumin),glucose_measurement=VALUES(glucose_measurement),urine_ph=VALUES(urine_ph),urine_gravity=VALUES(urine_gravity),urine_nitrite_test=VALUES(urine_nitrite_test),urine_leukocyte_esterace_test=VALUES(urine_leukocyte_esterace_test),urinary_ketone=VALUES(urinary_ketone),urine_bile_salt_test=VALUES(urine_bile_salt_test),
urine_bile_pigment_test=VALUES(urine_bile_pigment_test),urine_colour=VALUES(urine_colour),urine_turbidity=VALUES(urine_turbidity),urine_dipstick_for_blood=VALUES(urine_dipstick_for_blood)
;

END$$
DELIMITER ;

-- ------------- update etl_mch_postnatal_visit-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_mch_postnatal_visit$$
CREATE PROCEDURE sp_update_etl_mch_postnatal_visit()
BEGIN

insert into kenyaemr_etl.etl_mch_postnatal_visit(
patient_id,
uuid,
visit_id,
visit_date,
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
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),provider=VALUES(provider),temperature=VALUES(temperature),pulse_rate=VALUES(pulse_rate),systolic_bp=VALUES(systolic_bp),diastolic_bp=VALUES(diastolic_bp),respiratory_rate=VALUES(respiratory_rate),
oxygen_saturation=VALUES(oxygen_saturation),weight=VALUES(weight),height=VALUES(height),muac=VALUES(muac),hemoglobin=VALUES(hemoglobin),arv_status=VALUES(arv_status),general_condition=VALUES(general_condition),breast=VALUES(breast),cs_scar=VALUES(cs_scar),gravid_uterus=VALUES(gravid_uterus),episiotomy=VALUES(episiotomy),
lochia=VALUES(lochia),mother_hiv_status=VALUES(mother_hiv_status),condition_of_baby=VALUES(condition_of_baby),baby_feeding_method=VALUES(baby_feeding_method),umblical_cord=VALUES(umblical_cord),baby_immunization_started=VALUES(baby_immunization_started),family_planning_counseling=VALUES(family_planning_counseling),uterus_examination=VALUES(uterus_examination),
uterus_cervix_examination=VALUES(uterus_cervix_examination),vaginal_examination=VALUES(vaginal_examination),parametrial_examination=VALUES(parametrial_examination),external_genitalia_examination=VALUES(external_genitalia_examination),ovarian_examination=VALUES(ovarian_examination),pelvic_lymph_node_exam=VALUES(pelvic_lymph_node_exam)
;

END$$
DELIMITER ;

-- ------------- update etl_tb_enrollment-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_tb_enrollment$$
CREATE PROCEDURE sp_update_etl_tb_enrollment()
BEGIN

insert into kenyaemr_etl.etl_tb_enrollment(
patient_id,
uuid,
provider,
visit_id,
visit_date,
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
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),date_treatment_started=VALUES(date_treatment_started),district=VALUES(district),referred_by=VALUES(referred_by),referral_date=VALUES(referral_date),
date_transferred_in=VALUES(date_transferred_in),facility_transferred_from=VALUES(facility_transferred_from),district_transferred_from=VALUES(district_transferred_from),date_first_enrolled_in_tb_care=VALUES(date_first_enrolled_in_tb_care),weight=VALUES(weight),height=VALUES(height),treatment_supporter=VALUES(treatment_supporter),relation_to_patient=VALUES(relation_to_patient),
treatment_supporter_address=VALUES(treatment_supporter_address),treatment_supporter_phone_contact=VALUES(treatment_supporter_phone_contact),disease_classification=VALUES(disease_classification),patient_classification=VALUES(patient_classification),pulmonary_smear_result=VALUES(pulmonary_smear_result),has_extra_pulmonary_pleurial_effusion=VALUES(has_extra_pulmonary_pleurial_effusion),
has_extra_pulmonary_milliary=VALUES(has_extra_pulmonary_milliary),has_extra_pulmonary_lymph_node=VALUES(has_extra_pulmonary_lymph_node),has_extra_pulmonary_menengitis=VALUES(has_extra_pulmonary_menengitis),has_extra_pulmonary_skeleton=VALUES(has_extra_pulmonary_skeleton),has_extra_pulmonary_abdominal=VALUES(has_extra_pulmonary_abdominal)
;

END$$
DELIMITER ;

-- ------------- update etl_tb_follow_up_visit-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_tb_follow_up_visit$$
CREATE PROCEDURE sp_update_etl_tb_follow_up_visit()
BEGIN

insert into kenyaemr_etl.etl_tb_follow_up_visit(
patient_id,
uuid,
provider,
visit_id ,
visit_date ,
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
e.encounter_id,
max(if(o.concept_id=159961,o.value_coded,null)) as spatum_test,
max(if(o.concept_id=307,o.value_coded,null)) as spatum_result,
max(if(o.concept_id=159968,o.value_numeric,null)) as result_serial_number,
max(if(o.concept_id=160023,o.value_numeric,null)) as quantity,
max(if(o.concept_id=159964,o.value_datetime,null)) as date_test_done,
max(if(o.concept_id=159982,o.value_coded,null)) as bacterial_colonie_growth,
max(if(o.concept_id=159952,o.value_numeric,null)) as number_of_colonies,
max(if(o.concept_id=159956 and o.value_coded=84360,o.value_numeric,null)) as resistant_s,
max(if(o.concept_id=159956 and o.value_coded=767,o.value_text,null)) as resistant_r,
max(if(o.concept_id=159956 and o.value_coded=78280,o.value_coded,null)) as resistant_inh,
max(if(o.concept_id=159956 and o.value_coded=75948,o.value_text,null)) as resistant_e,
max(if(o.concept_id=159958 and o.value_coded=84360,o.value_text,null)) as sensitive_s,
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
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),spatum_test=VALUES(spatum_test),spatum_result=VALUES(spatum_result),result_serial_number=VALUES(result_serial_number),quantity=VALUES(quantity) ,date_test_done=VALUES(date_test_done),bacterial_colonie_growth=VALUES(bacterial_colonie_growth),
number_of_colonies=VALUES(number_of_colonies),resistant_s=VALUES(resistant_s),resistant_r=VALUES(resistant_r),resistant_inh=VALUES(resistant_inh),resistant_e=VALUES(resistant_e),sensitive_s=VALUES(sensitive_s),sensitive_r=VALUES(sensitive_r),sensitive_inh=VALUES(sensitive_inh),sensitive_e=VALUES(sensitive_e),test_date=VALUES(test_date),hiv_status=VALUES(hiv_status),next_appointment_date=VALUES(next_appointment_date)
;

END$$
DELIMITER ;

-- ------------- update etl_tb_screening-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_tb_screening$$
CREATE PROCEDURE sp_update_etl_tb_screening()
BEGIN

insert into kenyaemr_etl.etl_tb_screening(
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
cough_for_2wks_or_more,
confirmed_tb_contact,
chronic_cough,
fever_for_2wks_or_more,
noticeable_weight_loss,
chest_pain,
night_sweat_for_2wks_or_more,
resulting_tb_status ,
tb_treatment_start_date ,
notes 
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_datetime,
e.encounter_id,
max(if(o.concept_id=1728 and o.value_coded=159799,o.value_coded,null)) as cough_for_2wks_or_more,
max(if(o.concept_id=1728 and o.value_coded=124068,o.value_coded,null)) as confirmed_tb_contact,
max(if(o.concept_id=1728 and o.value_coded=145455,o.value_coded,null)) as chronic_cough,
max(if(o.concept_id=1728 and o.value_coded=1494,o.value_coded,null)) as fever_for_2wks_or_more,
max(if(o.concept_id=1728 and o.value_coded=832,o.value_coded,null)) as noticeable_weight_loss,
max(if(o.concept_id=1728 and o.value_coded=120749,o.value_coded,null)) as chest_pain,
max(if(o.concept_id=1728 and o.value_coded=133027,o.value_coded,null)) as night_sweat_for_2wks_or_more,
max(if(o.concept_id=1659,o.value_coded,null)) as resulting_tb_status,
max(if(o.concept_id=1113,o.value_datetime,null)) as tb_treatment_start_date,
max(if(o.concept_id=160632,o.value_text,null)) as notes
from encounter e 
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0 
and o.concept_id in(1727,1728,1659,1113,160632)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where 
	uuid in('ed6dacc9-0827-4c82-86be-53c0d8c449be')
) et on et.encounter_type_id=e.encounter_type
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),cough_for_2wks_or_more=VALUES(cough_for_2wks_or_more),confirmed_tb_contact=VALUES(confirmed_tb_contact),chronic_cough=VALUES(chronic_cough),fever_for_2wks_or_more=VALUES(fever_for_2wks_or_more),
noticeable_weight_loss=VALUES(noticeable_weight_loss),chest_pain=VALUES(chest_pain),night_sweat_for_2wks_or_more=VALUES(night_sweat_for_2wks_or_more),resulting_tb_status=VALUES(resulting_tb_status) ,tb_treatment_start_date=VALUES(tb_treatment_start_date),notes=VALUES(notes)
;

END$$
DELIMITER ;

-- ------------- update etl_hei_enrollment-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_hei_enrolment$$
CREATE PROCEDURE sp_update_etl_hei_enrolment()
BEGIN

insert into kenyaemr_etl.etl_hei_enrollment(
patient_id,
uuid,
provider,
visit_id,
visit_date,
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
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),child_exposed=VALUES(child_exposed),spd_number=VALUES(spd_number),birth_weight=VALUES(birth_weight),gestation_at_birth=VALUES(gestation_at_birth),date_first_seen=VALUES(date_first_seen),
birth_notification_number=VALUES(birth_notification_number),birth_certificate_number=VALUES(birth_certificate_number),need_for_special_care=VALUES(need_for_special_care),reason_for_special_care=VALUES(reason_for_special_care),referral_source=VALUES(referral_source),transfer_in=VALUES(transfer_in),transfer_in_date=VALUES(transfer_in_date),facility_transferred_from=VALUES(facility_transferred_from),
district_transferred_from=VALUES(district_transferred_from),date_first_enrolled_in_hei_care=VALUES(date_first_enrolled_in_hei_care),mother_breastfeeding=VALUES(mother_breastfeeding),TB_contact_history_in_household=VALUES(TB_contact_history_in_household),mother_alive=VALUES(mother_alive),mother_on_pmtct_drugs=VALUES(mother_on_pmtct_drugs),
mother_on_drug=VALUES(mother_on_drug),mother_on_art_at_infant_enrollment=VALUES(mother_on_art_at_infant_enrollment),mother_drug_regimen=VALUES(mother_drug_regimen),parent_ccc_number=VALUES(parent_ccc_number),mode_of_delivery=VALUES(mode_of_delivery),place_of_delivery=VALUES(place_of_delivery)
;  

END$$
DELIMITER ;

-- ------------- update etl_hei_follow_up_visit-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_hei_follow_up$$
CREATE PROCEDURE sp_update_etl_hei_follow_up()
BEGIN

insert into kenyaemr_etl.etl_hei_follow_up_visit(
patient_id,
uuid,
provider,
visit_id,
visit_date,
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
-- dna_pcr_contextual_status,
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
-- max(if(o.concept_id=162084,o.value_coded,null)) as dna_pcr_contextual_status,
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
	uuid in('bcc6da85-72f2-4291-b206-789b8186a021')
) et on et.encounter_type_id=e.encounter_type
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id 
ON DUPLICATE KEY UPDATE provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),weight=VALUES(weight),height=VALUES(height),infant_feeding=VALUES(infant_feeding),tb_assessment_outcome=VALUES(tb_assessment_outcome),social_smile_milestone=VALUES(social_smile_milestone),head_control_milestone=VALUES(head_control_milestone),
response_to_sound_milestone=VALUES(response_to_sound_milestone),hand_extension_milestone=VALUES(hand_extension_milestone),sitting_milestone=VALUES(sitting_milestone),walking_milestone=VALUES(walking_milestone),standing_milestone=VALUES(standing_milestone),talking_milestone=VALUES(talking_milestone),review_of_systems_developmental=VALUES(review_of_systems_developmental),
dna_pcr_result=VALUES(dna_pcr_result),first_antibody_result=VALUES(first_antibody_result),final_antibody_result=VALUES(final_antibody_result),
tetracycline_ointment_given=VALUES(tetracycline_ointment_given),pupil_examination=VALUES(pupil_examination),sight_examination=VALUES(sight_examination),squint=VALUES(squint),deworming_drug=VALUES(deworming_drug),dosage=VALUES(dosage),unit=VALUES(unit),next_appointment_date=VALUES(next_appointment_date)
; 

END$$
DELIMITER ;

-- ------------- update etl_mchs_delivery-------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_mch_delivery$$
CREATE PROCEDURE sp_update_etl_mch_delivery()
BEGIN

insert into kenyaemr_etl.etl_mchs_delivery(
patient_id,
uuid,
provider,
visit_id,
visit_date,
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
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
group by e.encounter_id
ON DUPLICATE KEY UPDATE provider=VALUES(provider),visit_id=VALUES(visit_id),visit_date=VALUES(visit_date),encounter_id=VALUES(encounter_id),data_entry_date=VALUES(data_entry_date),duration_of_pregnancy=VALUES(duration_of_pregnancy),mode_of_delivery=VALUES(mode_of_delivery),date_of_delivery=VALUES(date_of_delivery),blood_loss=VALUES(blood_loss),condition_of_mother=VALUES(condition_of_mother),
apgar_score_1min=VALUES(apgar_score_1min),apgar_score_5min=VALUES(apgar_score_5min),apgar_score_10min=VALUES(apgar_score_10min),resuscitation_done=VALUES(resuscitation_done),place_of_delivery=VALUES(place_of_delivery),delivery_assistant=VALUES(delivery_assistant),counseling_on_infant_feeding=VALUES(counseling_on_infant_feeding) ,counseling_on_exclusive_breastfeeding=VALUES(counseling_on_exclusive_breastfeeding),
counseling_on_infant_feeding_for_hiv_infected=VALUES(counseling_on_infant_feeding_for_hiv_infected),mother_decision=VALUES(mother_decision)
;

END$$
DELIMITER ;


-- ------------------------------ update drug event -------------------------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_drug_event$$
CREATE PROCEDURE sp_update_drug_event()
BEGIN

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
reason_discontinued_other
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
	

END) as regimen_line,
-- cs.concept_set, 
d.discontinued,
d.drugs,
d.discontinued_date,
d.discontinued_reason,
d.discontinued_reason_non_coded 
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
where o.voided=0 and cs.concept_set = 1085 and o.discontinued=1
group by o.discontinued_date

) d on d.patient_id = o.patient_id and d.start_date=o.start_date
where cs.concept_set = 1085 and (
	o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
	or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
	)
group by o.patient_id, o.start_date
ON DUPLICATE KEY UPDATE date_started=VALUES(date_started), regimen=VALUES(regimen), discontinued=VALUES(discontinued), regimen_discontinued=VALUES(regimen_discontinued),
date_discontinued=VALUES(date_discontinued), reason_discontinued=VALUES(reason_discontinued), reason_discontinued_other=VALUES(reason_discontinued_other)
;

END$$
DELIMITER ;

-- ------------- update etl_pharmacy_extract table--------------------------------
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_pharmacy_extract$$
CREATE PROCEDURE sp_update_etl_pharmacy_extract()
BEGIN
insert into kenyaemr_etl.etl_pharmacy_extract(
patient_id,
uuid,
visit_date,
visit_id,
encounter_id,
date_created,
encounter_name,
drug,
is_arv,
-- drug_name,
frequency,
duration,
unit,
voided,
date_voided,
dispensing_provider
)
select 
	o.person_id,
	max(if(o.concept_id=1282, o.uuid, null)),
	date(o.obs_datetime) as enc_date,
	e.visit_id,
	o.encounter_id,
	e.date_created,
	et.name as enc_name,
	max(if(o.concept_id = 1282 and o.value_coded is not null,o.value_coded, null)) as drug_dispensed,
	max(if(o.concept_id = 1282 and cs.concept_set=1085, 1, 0)) as arv_drug, -- arv:1085
	-- max(if(o.concept_id = 1282, cn.name, 0)) as drug_name, -- arv:1085
	max(if(o.concept_id = 1443, o.value_numeric, null)) as dose,
	max(if(o.concept_id = 159368, o.value_numeric, null)) as duration,
	max(if(o.concept_id = 1732 and o.value_coded=1072,'Days',if(o.concept_id=1732 and o.value_coded=1073,'Weeks',if(o.concept_id=1732 and o.value_coded=1074,'Months',null)))) as unit,
	o.voided,
	o.date_voided,
	e.creator
from obs o
left outer join encounter e on e.encounter_id = o.encounter_id and e.voided=0
left outer join encounter_type et on et.encounter_type_id = e.encounter_type
left outer join concept_name cn on o.value_coded = cn.concept_id and cn.locale='en' and cn.concept_name_type='SHORT' -- FULLY_SPECIFIED'
left outer join concept_set cs on o.value_coded = cs.concept_id 
where o.voided=0 and o.concept_id in(1282,1732,159368,1443,1444) and e.encounter_type not in (9,13) and 
(
	o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
	or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
	)
group by o.obs_group_id, o.person_id, encounter_id
having drug_dispensed is not null
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), encounter_name=VALUES(encounter_name), is_arv=VALUES(is_arv), frequency=VALUES(frequency),
duration=VALUES(duration), unit=VALUES(unit), voided=VALUES(voided), date_voided=VALUES(date_voided)
;

END$$

DELIMITER ;

-- ------------------------------------- laboratory updates ---------------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_update_etl_laboratory_extract$$
CREATE PROCEDURE sp_update_etl_laboratory_extract()
BEGIN

insert into kenyaemr_etl.etl_laboratory_extract(
uuid,
encounter_id,
patient_id,
visit_date,
visit_id,
lab_test,
test_result,
date_created,
created_by 
)
select 
o.uuid,
e.encounter_id,
e.patient_id,
e.encounter_datetime as visit_date,
e.visit_id,
o.concept_id,
(CASE when o.concept_id in(5497,730,654,790,856,21) then o.value_numeric
	when o.concept_id in(299,1030,302,32) then o.value_coded
	END) AS test_result,
e.date_created,
e.creator
from encounter e 
inner join obs o on e.encounter_id=o.encounter_id and o.voided=0
and o.concept_id in (5497,730,299,654,790,856,1030,21,302,32) -- (5497-N,730-N,299-C,654-N,790-N,856-N,1030-C,21-N,302-C,32-C)
inner join 
(
	select encounter_type_id, uuid, name from encounter_type where uuid ='17a381d1-7e29-406a-b782-aa903b963c28'
) et on et.encounter_type_id=e.encounter_type
where e.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_changed > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or e.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_created > (select max(stop_time) from kenyaemr_etl.etl_script_status)
or o.date_voided > (select max(stop_time) from kenyaemr_etl.etl_script_status)
ON DUPLICATE KEY UPDATE visit_date=VALUES(visit_date), lab_test=VALUES(lab_test), test_result=VALUES(test_result)
; 

END$$
DELIMITER ;


-- ----------------------------  scheduled updates ---------------------

DELIMITER $$
DROP PROCEDURE IF EXISTS sp_scheduled_updates$$
CREATE PROCEDURE sp_scheduled_updates()
BEGIN
DECLARE update_script_id INT(11);

INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('scheduled_updates', NOW());
SET update_script_id = LAST_INSERT_ID();

CALL sp_update_etl_patient_demographics();
CALL sp_update_etl_hiv_enrollment();
CALL sp_update_etl_hiv_followup();
CALL sp_update_etl_program_discontinuation();
CALL sp_update_etl_mch_enrollment();
CALL sp_update_etl_mch_antenatal_visit();
CALL sp_update_etl_mch_postnatal_visit();
CALL sp_update_etl_tb_enrollment();
CALL sp_update_etl_tb_follow_up_visit();
CALL sp_update_etl_tb_screening();
CALL sp_update_etl_hei_enrolment();
CALL sp_update_etl_hei_follow_up();
CALL sp_update_etl_mch_delivery();
CALL sp_update_drug_event();
CALL sp_update_etl_pharmacy_extract();
CALL sp_update_etl_laboratory_extract();

UPDATE kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= update_script_id;

END$$
DELIMITER ;

DELIMITER $$
DROP EVENT IF EXISTS event_update_kenyaemr_etl_tables$$
CREATE EVENT event_update_kenyaemr_etl_tables
	ON SCHEDULE EVERY 5 MINUTE STARTS CURRENT_TIMESTAMP
	DO
		CALL sp_scheduled_updates();
	$$
DELIMITER ;







