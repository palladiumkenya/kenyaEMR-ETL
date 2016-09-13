
DELIMITER $$
DROP PROCEDURE IF EXISTS create_etl_tables$$
CREATE PROCEDURE create_etl_tables()
BEGIN

-- create table etl_patient_demographics
drop table if exists etl_patient_demographics;

create table etl_patient_demographics (
patient_id INT(11) not null primary key,
given_name VARCHAR(50),
middle_name VARCHAR(50),
family_name VARCHAR(50),
Gender VARCHAR(10),
DOB DATE,
national_id_no VARCHAR(50),
unique_patient_no VARCHAR(50),
patient_clinic_number VARCHAR(15)
phone_number VARCHAR(50),
birth_place VARCHAR(50),
citizenship VARCHAR(50),
email_address VARCHAR(50),
next_of_kin VARCHAR(100),
next_of_kin_phone VARCHAR(20),
next_of_kin_relationship VARCHAR(50),
dead INT(11),
voided INT(11),
index(patient_id),
index(Gender),
index(unique_patient_no),
index(DOB)

);

-- create table etl_hiv_enrollment
drop table if exists etl_hiv_enrollment;

create table etl_hiv_enrollment(
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
patient_id INT(11) NOT NULL,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
encounter_provider INT(11),
date_of_enrollment DATE,
unique_patient_no VARCHAR(15),
patient_clinic_number VARCHAR(15),
date_first_enrolled_in_care	DATE,
entry_point	double,
transfer_in_date DATE,
facility_transferred_from VARCHAR(50),
district_transferred_from VARCHAR(50),
date_started_art_at_transferring_facility DATE,
date_confirmed_hiv_positive	DATE,
facility_confirmed_hiv_positive	VARCHAR(50),
arv_status double,
name_of_treatment_supporter	VARCHAR(50),
relationship_of_treatment_supporter	double,
treatment_supporter_telephone VARCHAR(15),
treatment_supporter_address	VARCHAR(100),
date_created DATE,
voided INT(11),
constraint foreign key(patient_id) references etl_patient_demographics(patient_id),
index(patient_id),
index(visit_id),
index(visit_date),
index(date_of_enrollment),
index(arv_status),
index(date_confirmed_hiv_positive)

);


END$$
DELIMITER ;


-- --------------------------------------------------------------- insert into etl tables -------------------------------------------


DELIMITER $$
DROP PROCEDURE IF EXISTS populate_etl_patient_demographics$$
CREATE PROCEDURE populate_etl_patient_demographics()
BEGIN
insert into etl_patient_demographics(
patient_id,
given_name,
middle_name,
family_name,
Gender,
DOB,
dead,
voided
)
select 
p.person_id,
p.given_name,
p.middle_name,
p.family_name,
p.gender,
p.birthdate,
p.dead
FROM (
select 
p.person_id,
pn.given_name,
pn.middle_name,
pn.family_name,
p.gender,
p.birthdate,
p.dead,
p.voided
from person p 
inner join person_name pn on pn.person_id = p.person_id and pn.voided=0
GROUP BY p.person_id
) p
ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name=p.middle_name, family_name=p.family_name;

update etl_patient_demographics d 
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


update etl_patient_demographics d 
left outer join 
(select 
	pi.patient_id,
	max(if(pit.uuid='05ee9cf4-7242-4a17-b4d4-00f707265c8a', pi.identifier, null)) as UPN,
	max(if(pit.uuid='b4d66522-11fc-45c7-83e3-39a1af21ae0d', pi.identifier, null)) as PCN,
	max(if(pit.uuid='49af6cdc-7968-4abb-bf46-de10d7f4859f', pi.identifier, null)) as national_id_no
	from patient_identifier pi
	inner join 
	(
	select 
	name, 
	patient_identifier_type_id, 
	uuid 
	from patient_identifier_type
	) pit on pit.patient_identifier_type_id = pi.identifier_type
		and pit.uuid in (
	'05ee9cf4-7242-4a17-b4d4-00f707265c8a', -- upn
	'b4d66522-11fc-45c7-83e3-39a1af21ae0d', -- pcn
	'49af6cdc-7968-4abb-bf46-de10d7f4859f' -- national-id
		)
group by pi.patient_id
) pit on pit.patient_id = d.patient_id
set d.unique_patient_no=pit.UPN, 
	d.national_id_no=pit.national_id_no,
	d.patient_clinic_number=pit.PCN



END$$

DELIMITER ;

patient_id
visit_id
visit_date
encounter_id
visit_scheduled
person_present
weight
systolic_pressure
diastolic_pressure
height
temperature
pulse_rate
respiratory_rate
oxygen_saturation
muac
who_stage
substitution_first_line_regimen_date
substitution_first_line_regimen_reason
substitution_second_line_regimen_date
substitution_second_line_regimen_reason
second_line_regimen_change_date
second_line_regimen_change_reason
pregnancy_status
pregnancy_outcome
anc_number
expected_delivery_date
last_menstrual_period
gavida
parity
family_planning_status
family_planning_method
reason_not_using_family_planning
tb_status
tb_treatment_no
ctx_adherence
ctx_dispensed
inh_dispensed
arv_adherence
poor_arv_adherence_reason
poor_arv_adherence_reason_other
pwp_disclosure
pwp_partner_tested
condom_provided
screened_for_sti
at_risk_population
next_appointment_date

encounter_id

-- populate patient_hiv_enrollment table
-- uuid: de78a6be-bfc5-4634-adc3-5f1a280455cc

insert into etl_hiv_enrollment (
patient_id,
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
e.visit_id,
e.encounter_datetime as visit_date,
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
inner join obs o on o.encounter_id=e.encounter_id 
	and o.concept_id in (160555,160540,160534,160535,161551,159599,160554,160632,160533,160638,160640,160642,160641)
where e.voided=0
group by e.patient_id, e.encounter_id
order by e.patient_id






