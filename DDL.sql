DELIMITER $$
DROP PROCEDURE IF EXISTS create_etl_tables$$
CREATE PROCEDURE create_etl_tables()
BEGIN
-- create/recreate database kenyaemr_etl
drop database if exists kenyaemr_etl;
create database kenyaemr_etl;

-- create table etl_patient_demographics
drop table if exists kenyaemr_etl.etl_hiv_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_hiv_followup;
DROP TABLE IF EXISTS kenyaemr_etl.etl_laboratory_extract;
DROP TABLE IF EXISTS kenyaemr_etl.etl_pharmacy_extract;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patient_treatment_event;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mch_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mch_antenatal_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mch_postnatal_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_tb_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_tb_follow_up_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_tb_screening;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hei_enrollment;
DROP TABLE IF EXISTS kenyaemr_etl.etl_hei_follow_up_visit;
DROP TABLE IF EXISTS kenyaemr_etl.etl_mchs_delivery;
DROP TABLE IF EXISTS kenyaemr_etl.etl_patients_booked_today;
DROP TABLE IF EXISTS kenyaemr_etl.etl_missed_appointments;
drop table if exists kenyaemr_etl.etl_patient_demographics;


create table kenyaemr_etl.etl_patient_demographics (
patient_id INT(11) not null primary key,
uuid CHAR(38) NOT NULL,
given_name VARCHAR(50),
middle_name VARCHAR(50),
family_name VARCHAR(50),
Gender VARCHAR(10),
DOB DATE,
national_id_no VARCHAR(50),
unique_patient_no VARCHAR(50),
patient_clinic_number VARCHAR(15) DEFAULT NULL,
phone_number VARCHAR(50) DEFAULT NULL,
birth_place VARCHAR(50) DEFAULT NULL,
citizenship VARCHAR(50) DEFAULT NULL,
email_address VARCHAR(50) DEFAULT NULL,
next_of_kin VARCHAR(100) DEFAULT NULL,
next_of_kin_phone VARCHAR(20) DEFAULT NULL,
next_of_kin_relationship VARCHAR(50) DEFAULT NULL,
dead INT(11),
death_date DATE DEFAULT NULL,
voided INT(11),
index(patient_id),
index(Gender),
index(unique_patient_no),
index(DOB)

);
-- create table etl_hiv_enrollment


create table kenyaemr_etl.etl_hiv_enrollment(
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL,
visit_id INT(11) DEFAULT NULL,
visit_date DATE,
encounter_id INT(11),
encounter_provider INT(11),
date_first_enrolled_in_care DATE,
entry_point INT(11),
transfer_in_date DATE,
facility_transferred_from VARCHAR(50),
district_transferred_from VARCHAR(50),
date_started_art_at_transferring_facility DATE,
date_confirmed_hiv_positive DATE,
facility_confirmed_hiv_positive VARCHAR(50),
arv_status INT(11),
name_of_treatment_supporter VARCHAR(50),
relationship_of_treatment_supporter INT(11),
treatment_supporter_telephone VARCHAR(15),
treatment_supporter_address VARCHAR(100),
date_created DATE,
voided INT(11),
constraint foreign key(patient_id) references etl_patient_demographics(patient_id),
index(patient_id),
index(visit_id),
index(visit_date),
index(date_started_art_at_transferring_facility),
index(arv_status),
index(date_confirmed_hiv_positive)

);

-- create table etl_hiv_followup

CREATE TABLE kenyaemr_etl.etl_patient_hiv_followup (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
encounter_id INT(11),
patient_id INT(11) NOT NULL ,
visit_date DATE,
visit_id INT(11),
encounter_provider INT(11),
date_created DATE,
visit_scheduled INT(11),
person_present INT(11),
weight DOUBLE,
systolic_pressure DOUBLE,
diastolic_pressure DOUBLE,
height DOUBLE,
temperature DOUBLE,
pulse_rate DOUBLE,
respiratory_rate DOUBLE,
oxygen_saturation DOUBLE,
muac DOUBLE,
who_stage INT(11),
substitution_first_line_regimen_date DATE ,
substitution_first_line_regimen_reason INT(11),
substitution_second_line_regimen_date DATE,
substitution_second_line_regimen_reason INT(11),
second_line_regimen_change_date DATE,
second_line_regimen_change_reason INT(11),
pregnancy_status INT(11),
pregnancy_outcome INT(11),
anc_number VARCHAR(50),
expected_delivery_date DATE,
last_menstrual_period DATE,
gravida INT(11),
parity INT(11),
family_planning_status INT(11),
family_planning_method INT(11),
reason_not_using_family_planning INT(11),
tb_status INT(11),
tb_treatment_no VARCHAR(50),
ctx_adherence INT(11),
ctx_dispensed INT(11),
inh_dispensed INT(11),
arv_adherence INT(11),
poor_arv_adherence_reason INT(11),
poor_arv_adherence_reason_other VARCHAR(100),
pwp_disclosure INT(11),
pwp_partner_tested INT(11),
condom_provided INT(11),
screened_for_sti INT(11),
at_risk_population INT(11),
next_appointment_date DATE,
voided INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(patient_id, visit_date)

);


-- ------- create table etl_laboratory_extract-----------------------------------------

CREATE TABLE kenyaemr_etl.etl_laboratory_extract (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
encounter_id INT(11),
patient_id INT(11) NOT NULL ,
visit_date DATE,
visit_id INT(11),
lab_test VARCHAR(200),
test_result VARCHAR(200),
date_test_requested DATE DEFAULT null,
date_test_result_received DATE,
test_requested_by INT(11),
date_created DATE,
created_by INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)

);

-- ------------ create table etl_pharmacy_extract-----------------------


CREATE TABLE kenyaemr_etl.etl_pharmacy_extract(
id INT(11) NOT NULL PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_date DATE,
visit_id INT(11),
encounter_id INT(11),
drug INT(11),
dose INT(11),
unit INT(11),
frequency INT(11),
duration INT(11),
duration_units INT(11) ,
prescription_provider VARCHAR(50),
dispensing_provider VARCHAR(50),
regimen VARCHAR(50),
adverse_effects VARCHAR(100),
date_of_refill DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)

);

-- ------------ create table etl_patient_treatment_event-----------------------

CREATE TABLE kenyaemr_etl.etl_patient_treatment_event(
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
discontinuation_reason INT(11),
date_died DATE,
transfer_facility VARCHAR(50),
transfer_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_mch_enrollment-----------------------

CREATE TABLE kenyaemr_etl.etl_mch_enrollment (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
anc_number VARCHAR(50),
gravida INT(11),
parity INT(11),
parity_abortion INT(11),
lmp DATE,
lmp_estimated INT(11),
edd_ultrasound DATE,
blood_group INT(11),
serology INT(11),
tb_screening INT(11),
bs_for_mps INT(11),
hiv_status INT(11),
hiv_test_date DATE,
partner_hiv_status INT(11),
partner_hiv_test_date DATE,
urine_microscopy VARCHAR(100),
urinary_albumin INT(11),
glucose_measurement INT(11),
urine_ph INT(11),
urine_gravity INT(11),
urine_nitrite_test INT(11),
urine_leukocyte_esterace_test INT(11),
urinary_ketone INT(11),
urine_bile_salt_test INT(11),
urine_bile_pigment_test INT(11),
urine_colour INT(11),
urine_turbidity INT(11),
urine_dipstick_for_blood INT(11),
date_of_discontinuation INT(11),
discontinuation_reason INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_mch_antenatal_visit-----------------------

CREATE TABLE kenyaemr_etl.etl_mch_antenatal_visit (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
provider INT(11),
temperature DOUBLE,
pulse_rate DOUBLE,
systolic_bp DOUBLE,
diastolic_bp DOUBLE,
respiratory_rate DOUBLE,
oxygen_saturation INT(11),
weight DOUBLE,
height DOUBLE,
muac DOUBLE,
hemoglobin DOUBLE,
pallor INT(11),
maturity INT(11),
fundal_height DOUBLE,
fetal_presentation INT(11),
lie INT(11),
fetal_heart_rate INT(11),
fetal_movement INT(11),
who_stage  INT(11),
cd4 INT(11),
arv_status INT(11),
urine_microscopy VARCHAR(100),
urinary_albumin INT(11),
glucose_measurement INT(11),
urine_ph INT(11),
urine_gravity INT(11),
urine_nitrite_test INT(11),
urine_leukocyte_esterace_test INT(11),
urinary_ketone INT(11),
urine_bile_salt_test INT(11),
urine_bile_pigment_test INT(11),
urine_colour INT(11),
urine_turbidity INT(11),
urine_dipstick_for_blood INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_mch_postnatal_visit-----------------------

CREATE TABLE kenyaemr_etl.etl_mch_postnatal_visit (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
provider INT(11),
temperature DOUBLE,
pulse_rate DOUBLE,
systolic_bp DOUBLE,
diastolic_bp DOUBLE,
respiratory_rate DOUBLE,
oxygen_saturation INT(11),
weight DOUBLE,
height DOUBLE,
muac DOUBLE,
hemoglobin DOUBLE,
arv_status INT(11),
general_condition INT(11),
breast INT(11),
cs_scar INT(11),
gravid_uterus INT(11),
episiotomy INT(11),
lochia INT(11),
mother_hiv_status INT(11),
condition_of_baby INT(11),
baby_feeding_method INT(11),
umblical_cord INT(11),
baby_immunization_started INT(11),
family_planning_counseling INT(11),
uterus_examination VARCHAR(100),
uterus_cervix_examination VARCHAR(100),
vaginal_examination VARCHAR(100),
parametrial_examination VARCHAR(100),
external_genitalia_examination VARCHAR(100),
ovarian_examination VARCHAR(100),
pelvic_lymph_node_exam VARCHAR(100),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);


-- ------------ create table etl_tb_enrollment-----------------------

CREATE TABLE kenyaemr_etl.etl_tb_enrollment (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
provider INT(11),
date_treatment_started DATE,
district VARCHAR(50),
district_registration_number VARCHAR(20),
referred_by INT(11),
referral_date DATE,
date_transferred_in DATE,
facility_transferred_from VARCHAR(50),
district_transferred_from VARCHAR(50),
date_first_enrolled_in_tb_care DATE,
weight DOUBLE,
height DOUBLE,
treatment_supporter VARCHAR(100),
relation_to_patient INT(11),
treatment_supporter_address VARCHAR(100),
treatment_supporter_phone_contact VARCHAR(100),
disease_classification INT(11),
patient_classification INT(11),
pulmonary_smear_result INT(11),
has_extra_pulmonary_pleurial_effusion INT(11),
has_extra_pulmonary_milliary INT(11),
has_extra_pulmonary_lymph_node INT(11),
has_extra_pulmonary_menengitis INT(11),
has_extra_pulmonary_skeleton INT(11),
has_extra_pulmonary_abdominal INT(11),
has_extra_pulmonary_other VARCHAR(100),
treatment_outcome INT(11),
treatment_outcome_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_tb_follow_up_visit-----------------------

CREATE TABLE kenyaemr_etl.etl_tb_follow_up_visit (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
spatum_test INT(11),
spatum_result INT(11),
result_serial_number VARCHAR(20),
quantity DOUBLE ,
date_test_done DATE,
bacterial_colonie_growth INT(11),
number_of_colonies DOUBLE,
resistant_s INT(11),
resistant_r INT(11),
resistant_inh INT(11),
resistant_e INT(11),
sensitive_s INT(11),
sensitive_r INT(11),
sensitive_inh INT(11),
sensitive_e INT(11),
test_date DATE,
hiv_status INT(11),
next_appointment_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_tb_screening-----------------------

CREATE TABLE kenyaemr_etl.etl_tb_screening (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
cough_for_2wks_or_more INT(11),
confirmed_tb_contact INT(11),
chronic_cough INT(11),
fever_for_2wks_or_more INT(11),
noticeable_weight_loss INT(11),
chest_pain INT(11),
night_sweat_for_2wks_or_more INT(11),
resulting_tb_status INT(11),
tb_treatment_start_date DATE,
notes VARCHAR(100),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_hei_enrollment-----------------------

CREATE TABLE kenyaemr_etl.etl_hei_enrollment (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
provider INT(11),
encounter_id INT(11),
child_exposed INT(11),
hei_id_number VARCHAR(50),
spd_number VARCHAR(50),
birth_weight DOUBLE,
gestation_at_birth DOUBLE,
date_first_seen DATE,
birth_notification_number VARCHAR(50),
birth_certificate_number VARCHAR(50),
need_for_special_care INT(11),
reason_for_special_care INT(11),
referral_source INT(11),
transfer_in INT(11),
transfer_in_date DATE,
facility_transferred_from VARCHAR(50),
district_transferred_from VARCHAR(50),
date_first_enrolled_in_hei_care DATE,
arv_prophylaxis INT(11),
mother_breastfeeding INT(11),
mother_on_NVP_during_breastfeeding INT(11),
TB_contact_history_in_household INT(11),
infant_mother_link INT(11),
mother_alive INT(11),
mother_on_pmtct_drugs INT(11),
mother_on_drug INT(11),
mother_on_art_at_infant_enrollment INT(11),
mother_drug_regimen INT(11),
parent_ccc_number VARCHAR(50),
mode_of_delivery INT(11),
place_of_delivery INT(11),
exit_date DATE,
exit_reason INT(11),
hiv_status_at_exit INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_hei_follow_up_visit-----------------------

CREATE TABLE kenyaemr_etl.etl_hei_follow_up_visit (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
weight DOUBLE,
height DOUBLE,
infant_feeding INT(11),
tb_assessment_outcome INT(11),
social_smile_milestone INT(11),
head_control_milestone INT(11),
response_to_sound_milestone INT(11),
hand_extension_milestone INT(11),
sitting_milestone INT(11),
walking_milestone INT(11),
standing_milestone INT(11),
talking_milestone INT(11),
review_of_systems_developmental INT(11),
dna_pcr_sample_date DATE,
dna_pcr_contextual_status INT(11),
dna_pcr_result INT(11),
dna_pcr_dbs_sample_code VARCHAR(100),
dna_pcr_results_date DATE,
first_antibody_sample_date DATE,
first_antibody_result INT(11),
first_antibody_dbs_sample_code VARCHAR(100),
first_antibody_result_date DATE,
final_antibody_sample_date DATE,
final_antibody_result INT(11),
final_antibody_dbs_sample_code VARCHAR(100),
final_antibody_result_date DATE,
tetracycline_ointment_given INT(11),
pupil_examination INT(11),
sight_examination INT(11),
squint INT(11),
deworming_drug INT(11),
dosage INT(11),
unit VARCHAR(100),
next_appointment_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_mchs_delivery-----------------------

CREATE TABLE kenyaemr_etl.etl_mchs_delivery (
id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
encounter_id INT(11),
data_entry_date DATE,
duration_of_pregnancy DOUBLE,
mode_of_delivery INT(11),
date_of_delivery DATE,
blood_loss INT(11),
condition_of_mother  VARCHAR(100),
apgar_score_1min  DOUBLE,
apgar_score_5min  DOUBLE,
apgar_score_10min DOUBLE,
resuscitation_done INT(11),
place_of_delivery INT(11),
delivery_assistant INT(11),
counseling_on_infant_feeding  INT(11),
counseling_on_exclusive_breastfeeding INT(11),
counseling_on_infant_feeding_for_hiv_infected INT(11),
mother_decision INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id)
);

-- ------------ create table etl_patients_booked_today-----------------------

CREATE TABLE kenyaemr_etl.etl_patients_booked_today(
id INT(11) NOT NULL PRIMARY KEY,
patient_id INT(11) NOT NULL ,
last_tca_date DATE,
last_visit_date DATE,
date_table_created DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(patient_id)
);

-- ------------ create table etl_missed_appointments-----------------------

CREATE TABLE kenyaemr_etl.etl_missed_appointments(
id INT(11) NOT NULL PRIMARY KEY,
patient_id INT(11) NOT NULL ,
last_tca_date DATE,
last_visit_date DATE,
last_encounter_type VARCHAR(100),
days_since_last_visit INT(11),
date_table_created DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES etl_patient_demographics(patient_id),
INDEX(patient_id)
);

END$$
DELIMITER ;
