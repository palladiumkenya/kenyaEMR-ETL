
DELIMITER $$
DROP PROCEDURE IF EXISTS create_datatools_tables$$
CREATE PROCEDURE create_datatools_tables()
BEGIN
-- create/recreate database kenyaemr_etl
SELECT "Recreating kenyaemr_datatools database";
DROP DATABASE IF EXISTS kenyaemr_datatools;
create database kenyaemr_datatools;

-- ------------------- create table to hold etl script progress ------------------

SELECT "Droping existing datatools tables";
DROP TABLE if exists kenyaemr_datatools.patient_demographics;
DROP TABLE if exists kenyaemr_datatools.hiv_enrollment;
DROP TABLE IF EXISTS kenyaemr_datatools.hiv_followup;
DROP TABLE IF EXISTS kenyaemr_datatools.laboratory_extract;
DROP TABLE IF EXISTS kenyaemr_datatools.pharmacy_extract;
DROP TABLE IF EXISTS kenyaemr_datatools.patient_program_discontinuation;
DROP TABLE IF EXISTS kenyaemr_datatools.mch_enrollment;
DROP TABLE IF EXISTS kenyaemr_datatools.mch_antenatal_visit ;
DROP TABLE IF EXISTS kenyaemr_datatools.mch_postnatal_visit;
DROP TABLE IF EXISTS kenyaemr_datatools.tb_enrollment;
DROP TABLE IF EXISTS kenyaemr_datatools.tb_follow_up_visit;
DROP TABLE IF EXISTS kenyaemr_datatools.tb_screening;
DROP TABLE IF EXISTS kenyaemr_datatools.hei_enrollment;
DROP TABLE IF EXISTS kenyaemr_datatools.hei_follow_up_visit;
DROP TABLE IF EXISTS kenyaemr_datatools.mch_delivery;
DROP TABLE IF EXISTS kenyaemr_datatools.mch_discharge;
DROP TABLE IF EXISTS kenyaemr_datatools.hts_test;
DROP TABLE IF EXISTS kenyaemr_datatools.hts_referral_and_linkage;
DROP TABLE IF EXISTS kenyaemr_datatools.current_in_care;
DROP TABLE IF EXISTS kenyaemr_datatools.drug_event;
DROP TABLE IF EXISTS kenyaemr_datatools.ipt_screening;
DROP TABLE IF EXISTS kenyaemr_datatools.ipt_followup;
SELECT "Recreating datatools tables";

-- populate patient_hiv_enrollment table
create table kenyaemr_datatools.patient_demographics as
select
patient_id,
given_name,
middle_name,
family_name,
Gender,
DOB,
national_id_no,
unique_patient_no,
patient_clinic_number,
Tb_no,
district_reg_no,
hei_no,
phone_number,
birth_place,
citizenship,
email_address,
next_of_kin,
next_of_kin_relationship,
marital_status,
education_level,
if(dead=1, "Yes", "NO") dead,
death_date,
voided
from kenyaemr_etl.etl_patient_demographics;
SELECT "Successfully created patient demographics table";
-- ADD INDICES

ALTER TABLE kenyaemr_datatools.patient_demographics ADD PRIMARY KEY(patient_id);

ALTER TABLE kenyaemr_datatools.patient_demographics ADD INDEX(Gender);
ALTER TABLE kenyaemr_datatools.patient_demographics ADD INDEX(DOB);

-- populate patient_hiv_enrollment table
create table kenyaemr_datatools.hiv_enrollment as
select
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
(case entry_point when 159938 then "HBTC" when 160539 then "VCT Site" when 159937 then "MCH" when 160536 then "IPD-Adult"
  when 160537 then "IPD-Child," when 160541 then "TB Clinic" when 160542 then "OPD" when 162050 then "CCC"
  when 160551 then "Self Test," when 5622 then "Other(eg STI)" else "" end) as entry_point,
transfer_in_date,
facility_transferred_from,
district_transferred_from,
date_started_art_at_transferring_facility,
date_confirmed_hiv_positive,
facility_confirmed_hiv_positive,
(case arv_status when 1 then "Yes" when 0 then "No" else "" end) as arv_status,
name_of_treatment_supporter,
(case relationship_of_treatment_supporter when 973 then "Grandparent" when 972 then "Sibling" when 160639 then "Guardian" when 1527 then "Parent"
  when 5617 then "Spouse" when 163565 then "Partner" when 5622 then "Other" else "" end) as relationship_of_treatment_supporter,
treatment_supporter_telephone,
treatment_supporter_address
from kenyaemr_etl.etl_hiv_enrollment;


ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(visit_id);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(date_started_art_at_transferring_facility);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(arv_status);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(date_confirmed_hiv_positive);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(entry_point);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(transfer_in_date);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(date_first_enrolled_in_care);
ALTER TABLE kenyaemr_datatools.hiv_enrollment ADD INDEX(transfer_in_date);

SELECT "Successfully created hiv_enrollment table";

-- create table hiv_followup
create table kenyaemr_datatools.hiv_followup as
select
patient_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
(case visit_scheduled when 1246 then "visit_scheduled" else "" end )as visit_scheduled,
(case person_present when 978 then "Self (SF)" when 161642 then "Treatment supporter (TS)" when 5622 then "Other" else "" end) as person_present,
weight,
systolic_pressure,
diastolic_pressure,
height,
temperature,
pulse_rate,
respiratory_rate,
oxygen_saturation,
muac,
(case nutritional_status when 1115 then "Normal" when 163302 then "Severe acute malnutrition" when 163303 then "Moderate acute malnutrition" when 114413 then "Overweight/Obese" else "" end) as nutritional_status,
(case population_type when 164928 then "General Population" when 164929 then "Key Population" else "" end) as population_type,
(case key_population_type when 105 then "People who inject drugs" when 160578 then "Men who have sex with men" when 160579 then "Female sex Worker" else "" end) as key_population_type,
IF(who_stage in (1204,1204),"WHO Stage1", IF(who_stage in (1205,1221),"WHO Stage2", IF(who_stage in (1206,1222),"WHO Stage3", IF(who_stage in (1207,1223),"WHO Stage4", "")))) as who_stage,
(case presenting_complaints when 1 then "Yes" when 0 then "No" else "" end) as presenting_complaints,
clinical_notes,
(case on_anti_tb_drugs when 1065 then "Yes" when 1066 then "No" else "" end) as on_anti_tb_drugs,
(case on_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as on_ipt,
(case ever_on_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as ever_on_ipt,
(case spatum_smear_ordered when 1065 then "Yes" when 1066 then "No" else "" end) as spatum_smear_ordered,
(case chest_xray_ordered when 1065 then "Yes" when 1066 then "No" else "" end) as chest_xray_ordered,
(case genexpert_ordered when 1065 then "Yes" when 1066 then "No" else "" end) as genexpert_ordered,
(case spatum_smear_result when 703 then "POSITIVE" when 664 then "NEGATIVE" else "" end) as spatum_smear_result,
(case chest_xray_result when 1115 then "NORMAL" when 152526 then "ABNORMAL" else "" end) as chest_xray_result,
(case genexpert_result when 664 then "NEGATIVE" when 162203 then "Mycobacterium tuberculosis detected with rifampin resistance" when 162204 then "Mycobacterium tuberculosis detected without rifampin resistance"
  when 164104 then "Mycobacterium tuberculosis detected with indeterminate rifampin resistance"  when 163611 then "Invalid" when 1138 then "INDETERMINATE" else "" end) as genexpert_result,
(case referral when 1065 then "Yes" when 1066 then "No" else "" end) as referral,
(case clinical_tb_diagnosis when 703 then "POSITIVE" when 664 then "NEGATIVE" else "" end) as clinical_tb_diagnosis,
(case contact_invitation when 1065 then "Yes" when 1066 then "No" else "" end) as contact_invitation,
(case evaluated_for_ipt when 1065 then "Yes" when 1066 then "No" else "" end) as evaluated_for_ipt,
(case has_known_allergies when 1 then "Yes" when 0 then "No" else "" end) as has_known_allergies,
(case has_chronic_illnesses_cormobidities when 1065 then "Yes" when 1066 then "No" else "" end) as has_chronic_illnesses_cormobidities,
(case has_adverse_drug_reaction when 1 then "Yes" when 0 then "No" else "" end) as has_adverse_drug_reaction,
(case pregnancy_status when 1065 then "Yes" when 1066 then "No" else "" end) as pregnancy_status,
(case wants_pregnancy when 1065 then "Yes" when 1066 then "No" else "" end) as wants_pregnancy,
(case pregnancy_outcome when 126127 then "Spontaneous abortion" when 125872 then "STILLBIRTH" when 1395 then "Term birth of newborn" when 129218 then "Preterm Delivery (Maternal Condition)"
 when 159896 then "Therapeutic abortion procedure" when 151849 then "Liveborn, Unspecified Whether Single, Twin, or Multiple" when 1067 then "Unknown" else "" end) as pregnancy_outcome,anc_number,
expected_delivery_date,
last_menstrual_period,
gravida,
parity,
full_term_pregnancies,
abortion_miscarriages,
(case family_planning_status when 965 then "On Family Planning" when 160652 then "Not using Family Planning" when 1360 then "Wants Family Planning" else "" end) as family_planning_status,
(case family_planning_method when 160570 then "Emergency contraceptive pills" when 780 then "Oral Contraceptives Pills" when 5279 then "Injectible" when 1359 then "Implant"
when 5275 then "Intrauterine Device" when 136163 then "Lactational Amenorhea Method" when 5278 then "Diaphram/Cervical Cap" when 5277 then "Fertility Awareness"
when 1472 then "Tubal Ligation" when 190 then "Condoms" when 1489 then "Vasectomy" when 162332 then "Undecided" else "" end) as family_planning_method,
(case reason_not_using_family_planning when 160572 then "Thinks can't get pregnant" when 160573 then "Not sexually active now" when 5622 then "Other" else "" end) as reason_not_using_family_planning,
(case tb_status when 1660 then "No TB Signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "TB Screening Not Done"  else "" end) as tb_status,
tb_treatment_no,
(case ctx_adherence when 159405 then "Good" when 163794 then "Inadequate" when 159407 then "Poor" else "" end) as ctx_adherence,
(case ctx_dispensed when 1065 then "Yes" when 1066 then "No" when 1175 then "Not applicable" else "" end) as ctx_dispensed,
(case dapsone_adherence when 159405 then "Good" when 163794 then "Inadequate" when 159407 then "Poor" else "" end) as dapsone_adherence,
(case dapsone_dispensed when 1065 then "Yes" when 1066 then "No" when 1175 then "Not applicable" else "" end) as dapsone_dispensed,
(case inh_dispensed when 1065 then "Yes" when 1066 then "No" when 1175 then "Not applicable" else "" end) as inh_dispensed,
(case arv_adherence when 159405 then "Good" when 163794 then "Inadequate" when 159407 then "Poor" else "" end) as arv_adherence,
(case poor_arv_adherence_reason when 102 then "Toxicity, drug" when 121725 then "Alcohol abuse" when 119537 then "Depression"
when 5622 then "Other" when 1754 then "Medications unavailable" when 1778 then "TREATMENT OR PROCEDURE NOT CARRIED OUT DUE TO FEAR OF SIDE EFFECTS"
when 819 then "Cannot afford treatment" when 160583 then "Shares medications with others" when 160584 then "Lost or ran out of medication"
when 160585 then "Felt too ill to take medication" when 160586 then "Felt better and stopped taking medication" when 160587 then "Forgot to take medication"
when 160588 then "Pill burden" when 160589 then "Concerned about privacy/stigma" when 820 then "TRANSPORT PROBLEMS"  else "" end) as poor_arv_adherence_reason,
poor_arv_adherence_reason_other,
(case pwp_disclosure when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" when 1175 then "N/A" else "" end) as pwp_disclosure,
(case pwp_partner_tested when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" when 1175 then "N/A" else "" end) as pwp_partner_tested,
(case condom_provided when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" when 1175 then "N/A" else "" end) as condom_provided,
(case screened_for_sti when 703 then "POSITIVE" when 664 then "NEGATIVE" when 1118 then "Not Done" when 1175 then "N/A" else "" end) as screened_for_sti,
(case cacx_screening when 703 then "POSITIVE" when 664 then "NEGATIVE" when 1118 then "Not Done" when 1175 then "N/A" else "" end) as cacx_screening,
(case sti_partner_notification when 1065 then "Yes" when 1066 then "No" else "" end) as sti_partner_notification,
(case at_risk_population when 105 then "People who inject drugs" when 160578 then "Men who have sex with men" when 160579 then "Female sex Worker" else "" end) as at_risk_population,
(case system_review_finding when 1115 then "NORMAL" when 1116 then "ABNORMAL" else "" end) as system_review_finding,
next_appointment_date,
(case next_appointment_reason when 160523 then "Follow up" when 1283 then "Lab tests" when 159382 then "Counseling" when 160521 then "Pharmacy Refill" when 5622 then "Other"  else "" end) as next_appointment_reason,
(case differentiated_care when 164942 then "Standard Care" when 164943 then "Fast Track" when 164944 then "Community ART Distribution - HCW Led" when 164945 then "Community ART Distribution - Peer Led"
when 164946 then "Facility ART Distribution Group" else "" end) as differentiated_care
from kenyaemr_etl.etl_patient_hiv_followup;

ALTER TABLE kenyaemr_datatools.hiv_followup ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(encounter_id);
-- ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(patient_id,visit_date);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(who_stage);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(pregnancy_status);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(pregnancy_outcome);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(family_planning_status);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(family_planning_method);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(tb_status);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(condom_provided);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(ctx_dispensed);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(inh_dispensed);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(at_risk_population);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(population_type);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(key_population_type);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(on_anti_tb_drugs);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(on_ipt);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(ever_on_ipt);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(differentiated_care);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(visit_date,patient_id);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(visit_date,condom_provided);
ALTER TABLE kenyaemr_datatools.hiv_followup ADD INDEX(visit_date,family_planning_method);

SELECT "Successfully created hiv_followup table";

-- create table laboratory_extract
create table kenyaemr_datatools.laboratory_extract as
select
uuid,
encounter_id,
patient_id,
location_id,
visit_date,
visit_id,
(case lab_test when 5497 then "CD4 Count" when 730 then "CD4 PERCENT " when 654 then " 	SERUM GLUTAMIC-PYRUVIC TRANSAMINASE (ALT)" when 790 then "Serum creatinine (umol/L)"
  when 856 then "HIV VIRAL LOAD" when 21 then "Hemoglobin (HGB)" else "" end) as lab_test,
if(lab_test=299, (case test_result when 1228 then "REACTIVE" when 1229 then "NON-REACTIVE" when 1304 then "POOR SAMPLE QUALITY" end),
if(lab_test=1030, (case test_result when 1138 then "INDETERMINATE" when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" end),
if(lab_test=302, (case test_result when 1115 then "Normal" when 1116 then "Abnormal" when 1067 then "Unknown" end),
if(lab_test=32, (case test_result when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1138 then "INDETERMINATE" end),
if(lab_test=1305, (case test_result when 1306 then "BEYOND DETECTABLE LIMIT" when 1301 then "DETECTED" when 1302 then "NOT DETECTED" when 1304 then "POOR SAMPLE QUALITY" end),
test_result ))))) AS test_result,

date_created,
created_by
from kenyaemr_etl.etl_laboratory_extract;

ALTER TABLE kenyaemr_datatools.laboratory_extract ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.laboratory_extract ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.laboratory_extract ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.laboratory_extract ADD INDEX(lab_test);
ALTER TABLE kenyaemr_datatools.laboratory_extract ADD INDEX(test_result);

SELECT "Successfully created laboratory_extract table";

-- create table pharmacy_extract
create table kenyaemr_datatools.pharmacy_extract as
select
patient_id,
uuid,
visit_date,
visit_id,
encounter_id,
date_created,
encounter_name,
drug,
drug_name,
(case is_arv when 1 then "Yes" else "No" end) as is_arv,
(case is_ctx when 105281 then "SULFAMETHOXAZOLE / TRIMETHOPRIM (CTX)" else "" end) as is_ctx,
(case is_dapsone when 74250 then "DAPSONE" else "" end) as is_dapsone,
frequency,
duration,
duration_units,
voided,
date_voided,
dispensing_provider
from kenyaemr_etl.etl_pharmacy_extract;

ALTER TABLE kenyaemr_datatools.pharmacy_extract ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.pharmacy_extract ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.pharmacy_extract ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.pharmacy_extract ADD INDEX(drug_name);
SELECT "Successfully created pharmacy_extract table";

-- create table patient_program_discontinuation
create table kenyaemr_datatools.patient_program_discontinuation as
select
patient_id,
uuid,
visit_id,
visit_date,
program_uuid,
program_name,
encounter_id,
(case discontinuation_reason when 159492 then "Transferred Out" when 160034 then "Died" when 5240 then "Lost to Follow" when 819 then "Cannot afford Treatment"
  when 5622 then "Other" when 1067 then "Unknown" else "" end) as discontinuation_reason,
date_died,
transfer_facility,
transfer_date
from kenyaemr_etl.etl_patient_program_discontinuation;

ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(visit_date,program_name);
ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(visit_date,patient_id);
ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(discontinuation_reason);
ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(date_died);
ALTER TABLE kenyaemr_datatools.patient_program_discontinuation ADD INDEX(transfer_date);

SELECT "Successfully created patient_program_discontinuation table";

-- create table mch_enrollment
create table kenyaemr_datatools.mch_enrollment as
select
patient_id,
uuid,
visit_id,
visit_date,
location_id,
encounter_id,
anc_number,
first_anc_visit_date,
gravida,
parity,
parity_abortion,
age_at_menarche,
lmp,
lmp_estimated,
edd_ultrasound,
(case blood_group when 690 then "A POSITIVE" when 692 then "A NEGATIVE" when 694 then "B POSITIVE" when 696 then "B NEGATIVE" when 699 then "O POSITIVE"
 when 701 then "O NEGATIVE" when 1230 then "AB POSITIVE" when 1231 then "AB NEGATIVE" else "" end) as blood_group,
(case serology when 1228 then "REACTIVE" when 1229 then "NON-REACTIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as serology,
(case tb_screening when 664 then "NEGATIVE" when 703 then "POSITIVE" else "" end) as tb_screening,
(case bs_for_mps when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1138 then "INDETERMINATE" else "" end) as bs_for_mps,
(case hiv_status when 664 then "HIV Negative" when 703 then "HIV Positive" when 1402 then "Not Tested" else "" end) as hiv_status,
hiv_test_date,
(case partner_hiv_status when 664 then "HIV Negative" when 703 then "HIV Positive" when 1067 then "Unknown" else "" end) as partner_hiv_status,
partner_hiv_test_date,
urine_microscopy,
(case urinary_albumin when 664 then "Negative" when 1874 then "Trace - 15" when 1362 then "One Plus(+) - 30" when 1363 then "Two Plus(++) - 100" when 1364 then "Three Plus(+++) - 300" when 1365 then "Four Plus(++++) - 1000" else "" end) as urinary_albumin,
(case glucose_measurement when 1115 then "Normal" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" when 1365 then "Four Plus(++++)" else "" end) as glucose_measurement,
urine_ph,
urine_gravity,
(case urine_nitrite_test when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" else "" end) as urine_nitrite_test,
(case urine_leukocyte_esterace_test when 664 then "NEGATIVE" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_leukocyte_esterace_test,
(case urinary_ketone when 664 then "NEGATIVE" when 1874 then "Trace - 5" when 1362 then "One Plus(+) - 15" when 1363 then "Two Plus(++) - 50" when 1364 then "Three Plus(+++) - 150" else "" end) as urinary_ketone,
(case urine_bile_salt_test when 1115 then "Normal" when 1874 then "Trace - 1" when 1362 then "One Plus(+) - 4" when 1363 then "Two Plus(++) - 8" when 1364 then "Three Plus(+++) - 12" else "" end) as urine_bile_salt_test,
(case urine_bile_pigment_test when 664 then "NEGATIVE" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_bile_pigment_test,
(case urine_colour when 162099 then "Colourless" when 127778 then "Red color" when 162097 then "Light yellow colour" when 162105 then "Yellow-green colour" when 162098 then "Dark yellow colour" when 162100 then "Brown color" else "" end) as urine_colour,
(case urine_turbidity when 162102 then "Urine appears clear" when 162103 then "Cloudy urine" when 162104 then "Urine appears turbid" else "" end) as urine_turbidity,
(case urine_dipstick_for_blood when 664 then "NEGATIVE" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_dipstick_for_blood,
(case discontinuation_reason when 159492 then "Transferred out" when 1067 then "Unknown" when 160034 then "Died" when 5622 then "Other" when 819 then "819" else "" end) as discontinuation_reason
from kenyaemr_etl.etl_mch_enrollment;

ALTER TABLE kenyaemr_datatools.mch_enrollment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.mch_enrollment ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.mch_enrollment ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.mch_enrollment ADD INDEX(tb_screening);
ALTER TABLE kenyaemr_datatools.mch_enrollment ADD INDEX(hiv_status);
ALTER TABLE kenyaemr_datatools.mch_enrollment ADD INDEX(hiv_test_date);
ALTER TABLE kenyaemr_datatools.mch_enrollment ADD INDEX(partner_hiv_status);

SELECT "Successfully created mch_enrollment table";

-- create table mch_antenatal_visit
create table kenyaemr_datatools.mch_antenatal_visit as
select
patient_id,
uuid,
visit_id,
visit_date,
location_id,
encounter_id,
provider,
anc_visit_number,
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
(case breast_exam_done when 1065 then "Yes" when 1066 then "No" else "" end) as breast_exam_done,
(case pallor when 1065 then "Yes" when 1066 then "No" else "" end) as pallor,
maturity,
fundal_height,
(case fetal_presentation when 139814 then "Frank Breech Presentation" when 160091 then "vertex presentation" when 144433 then "Compound Presentation" when 115808 then "Mentum Presentation of Fetus"
 when 118388 then "Face or Brow Presentation of Foetus" when 129192 then "Presentation of Cord" when 112259 then "Transverse or Oblique Fetal Presentation" when 164148 then "Occiput Anterior Position"
 when 164149 then "Brow Presentation" when 164150 then "Face Presentation" when 156352 then "footling breech presentation" else "" end) as fetal_presentation,
(case lie when 132623 then "Oblique lie" when 162088 then "Longitudinal lie" when 124261 then "Transverse lie" else "" end) as lie,
fetal_heart_rate,
(case fetal_movement when 162090 then "Increased fetal movements" when 113377 then "Decreased fetal movements" when 1452 then "No fetal movements" when 162108 then "Fetal movements present" else "" end) as fetal_movement,
(case who_stage when 1204 then "WHO Stage1" when 1205 then "WHO Stage2" when 1206 then "WHO Stage3" when 1207 then "WHO Stage4" else "" end) as who_stage,
coalesce(v.viral_load,case v.ldl when 1302 then "ldl" else "" end) as viral_loadcd4,
(case arv_status when 1148 then "ARV Prophylaxis" when 1149 then "HAART" when 1175 then "NA" else "" end) as arv_status,
final_test_result,
patient_given_result,
partner_hiv_tested,
partner_hiv_status,
(case prophylaxis_given when 105281 then "Cotrimoxazole" when 74250 then "Dapsone" when 1107 then "None" else "" end) as prophylaxis_given,
(case azt_dispensed when 160123 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end) as azt_dispensed,
(case nvp_dispensed when 80586 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end) as nvp_dispensed,
deworming,
urine_microscopy,
(case urinary_albumin when 664 then "Negative" when 1874 then "Trace - 15" when 1362 then "One Plus(+) - 30" when 1363 then "Two Plus(++) - 100" when 1364 then "Three Plus(+++) - 300" when 1365 then "Four Plus(++++) - 1000" else "" end) as urinary_albumin,
(case glucose_measurement when 1115 then "Normal" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" when 1365 then "Four Plus(++++)" else "" end) as glucose_measurement,
urine_ph,
urine_gravity,
(case urine_nitrite_test when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" else "" end) as urine_nitrite_test,
(case urine_leukocyte_esterace_test when 664 then "NEGATIVE" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_leukocyte_esterace_test,
(case urinary_ketone when 664 then "NEGATIVE" when 1874 then "Trace - 5" when 1362 then "One Plus(+) - 15" when 1363 then "Two Plus(++) - 50" when 1364 then "Three Plus(+++) - 150" else "" end) as urinary_ketone,
(case urine_bile_salt_test when 1115 then "Normal" when 1874 then "Trace - 1" when 1362 then "One Plus(+) - 4" when 1363 then "Two Plus(++) - 8" when 1364 then "Three Plus(+++) - 12" else "" end) as urine_bile_salt_test,
(case urine_bile_pigment_test when 664 then "NEGATIVE" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_bile_pigment_test,
(case urine_colour when 162099 then "Colourless" when 127778 then "Red color" when 162097 then "Light yellow colour" when 162105 then "Yellow-green colour" when 162098 then "Dark yellow colour" when 162100 then "Brown color" else "" end) as urine_colour,
(case urine_turbidity when 162102 then "Urine appears clear" when 162103 then "Cloudy urine" when 162104 then "Urine appears turbid" else "" end) as urine_turbidity,
(case urine_dipstick_for_blood when 664 then "NEGATIVE" when 1874 then "Trace" when 1362 then "One Plus(+)" when 1363 then "Two Plus(++)" when 1364 then "Three Plus(+++)" else "" end) as urine_dipstick_for_blood,
(case syphilis_test_status when 1229 then "Non Reactive" when 1228 then "Reactive" when 1402 then "Not Tested" when 1304 then "Poor Sample quality" else "" end) as syphilis_test_status,
(case syphilis_treated_status when 1065 then "Yes" when 1066 then "No" else "" end) as syphilis_treated_status,
(case bs_mps when 664 then "Negative" when 703 then "Positive" when 1138 then "Indeterminate" else "" end) as bs_mps,
(case anc_exercises when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as anc_exercises,
(case tb_screening when 1660 then "No TB signs" when 164128 then "No signs and started on INH" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "Not done" when 1111 then "On TB Treatment"  else "" end) as tb_screening,
(case cacx_screening when 703 then "POSITIVE" when 664 then "NEGATIVE" when 159393 then "Presumed" when 1118 then "Not Done" when 1175 then "N/A" else "" end) as cacx_screening,
(case cacx_screening_method when 885 then "PAP Smear" when 162816 then "VIA" when 5622 then "Other" else "" end) as cacx_screening_method,
(case has_other_illnes  when 1065 then "Yes" when 1066 then "No" else "" end) as has_other_illnes,
(case counselled  when 1065 then "Yes" when 1066 then "No" else "" end) as counselled,
(case referred_from when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_from,
(case referred_to when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_to,
next_appointment_date,
clinical_notes

from kenyaemr_etl.etl_mch_antenatal_visit;

ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(who_stage);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(arv_status);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(cd4);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(final_test_result);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(test_1_kit_name);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(test_2_kit_name);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(tb_screening);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(syphilis_test_status);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(cacx_screening);
ALTER TABLE kenyaemr_datatools.mch_antenatal_visit ADD INDEX(next_appointment_date);

SELECT "Successfully created mch_antenatal_visit table";

  -- create table mch_discharge table
create table kenyaemr_datatools.mch_discharge as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
location_id,
encounter_id,
data_entry_date,
(case counselled_on_feeding when 1065 then "Yes" when 1066 then "No" else "" end) as counselled_on_feeding,
(case baby_status when 163016 then "Alive" when 160432 then "Dead" else "" end) as baby_status,
(case vitamin_A_dispensed when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as vitamin_A_dispensed,
birth_notification_number,
maternal_condition,
discharge_date,
(case referred_from when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_from,
(case referred_to when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_to,
clinical_notes

from kenyaemr_etl.etl_mchs_discharge;

ALTER TABLE kenyaemr_datatools.mch_discharge ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.mch_discharge ADD INDEX(patient_id);
ALTER TABLE kenyaemr_datatools.mch_discharge ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.mch_discharge ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.mch_discharge ADD INDEX(baby_status);
ALTER TABLE kenyaemr_datatools.mch_discharge ADD INDEX(discharge_date);

SELECT "Successfully created mch_discharge table";

-- create table mch_postnatal_visit
create table kenyaemr_datatools.mch_postnatal_visit as
select
patient_id,
uuid,
visit_id,
visit_date,
location_id,
encounter_id,
provider,
pnc_register_no,
pnc_visit_no,
delivery_date,
(case mode_of_delivery when 1170 then "SVD" when 1171 then "C-Section" else "" end) as mode_of_delivery,
(case place_of_delivery when 1589 then "Facility" when 1536 then "Home" when 5622 then "Other" else "" end) as place_of_delivery,
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
(case arv_status when 1148 then "ARV Prophylaxis" when 1149 then "HAART" when 1175 then "NA" else "" end) as arv_status,
(case general_condition when 1855 then "Good" when 162133 then "Fair" when 162132 then "Poor" else "" end) as general_condition,
(case breast when 1855 then "Good" when 162133 then "Fair" when 162132 then "Poor" else "" end) as breast,    -- recheck
(case cs_scar when 156794 then "infection of obstetric surgical wound" when 145776 then "Caesarean Wound Disruption" when 162129 then "Wound intact and healing" when 162130 then "Surgical wound healed" else "" end) as cs_scar,
(case gravid_uterus when 162111 then "On exam, uterine fundus 12-16 week size" when 162112 then "On exam, uterine fundus 16-20 week size" when 162113 then "On exam, uterine fundus 20-24 week size" when 162114 then "On exam, uterine fundus 24-28 week size"
 when 162115 then "On exam, uterine fundus 28-32 week size" when 162116 then "On exam, uterine fundus 32-34 week size" when 162117 then "On exam, uterine fundus 34-36 week size" when 162118 then "On exam, uterine fundus 36-38 week size"
 when 162119 then "On exam, uterine fundus 38 weeks-term size" when 123427 then "Uterus Involuted"  else "" end) as gravid_uterus,
(case episiotomy when 159842 then "repaired, episiotomy wound" when 159843 then "healed, episiotomy wound" when 159841 then "gap, episiotomy wound" when 113919 then "Postoperative Wound Infection" else "" end) as episiotomy,
(case lochia when 159845 then "lochia excessive" when 159846 then "lochia foul smelling" when 159721 then "Lochia type" else "" end) as lochia,  -- recheck
(case pallor when 1065 then "Yes" when 1066 then "No" when 1175 then "Not applicable" else "" end) as pallor,
(case pph when 1065 then "Present" when 1066 then "Absent" else "" end) as pph,
(case mother_hiv_status when 1067 then "Unknown" when 664 then "NEGATIVE" when 703 then "POSITIVE" else "" end) as mother_hiv_status,
(case condition_of_baby when 1855 then "In good health" when 162132 then "Patient condition poor" when 1067 then "Unknown" when 162133 then "Patient condition fair/satisfactory" else "" end) as condition_of_baby,
(case baby_feeding_method when 5526 then "BREASTFED EXCLUSIVELY" when 1595 then "REPLACEMENT FEEDING" when 6046 then "Mixed feeding" when 159418 then "Not at all sure" else "" end) as baby_feeding_method,
(case umblical_cord when 162122 then "Neonatal umbilical stump clean" when 162123 then "Neonatal umbilical stump not clean" when 162124 then "Neonatal umbilical stump moist" when 159418 then "Not at all sure" else "" end) as umblical_cord,
(case baby_immunization_started when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as baby_immunization_started,
(case family_planning_counseling when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as family_planning_counseling,
uterus_examination,
uterus_cervix_examination,
vaginal_examination,
parametrial_examination,
external_genitalia_examination,
ovarian_examination,
pelvic_lymph_node_exam,
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
partner_hiv_tested,
partner_hiv_status,
(case prophylaxis_given when 105281 then "Cotrimoxazole" when 74250 then "Dapsone" when 1107 then "None" else "" end) as prophylaxis_given,
(case haart_given_anc when 1 then "Yes" when 2 then "No" else "" end) as haart_given_anc,
(case haart_given_mat when 1 then "Yes" when 2 then "No" else "" end) as haart_given_mat,
haart_start_date,
(case azt_dispensed when 160123 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end) as azt_dispensed,
(case nvp_dispensed when 80586 then "Yes" when 1066 then "No" when 1175 then "N/A" else "" end) as nvp_dispensed,
(case maternal_condition_coded when 130 then "Puerperal sepsis" when 114244 then "Perineal Laceration" when 1855 then "In good health" when 134612 then "Maternal Death" when 160429 then "Alive" when 162132 then "Patient condition poor" when 162133 then "Patient condition fair/satisfactory" else "" end) as maternal_condition_coded,
(case iron_supplementation when 1065 then "Yes" when 1066 then "No" else "" end) as iron_supplementation,
(case fistula_screening when 1107 then "None" when 49 then "Vesicovaginal Fistula" when 127847 then "Rectovaginal fistula" when 1118 then "Not done"  else "" end) as fistula_screening,
(case cacx_screening when 703 then "POSITIVE" when 664 then "NEGATIVE" when 159393 then "Presumed" when 1118 then "Not Done" when 1175 then "N/A" else "" end) as cacx_screening,
(case cacx_screening_method when 885 then "PAP Smear" when 162816 then "VIA" when 5622 then "Other" else "" end) as cacx_screening_method,
(case family_planning_status when 965 then "On Family Planning" when 160652 then "Not using Family Planning"  else "" end) as family_planning_status,
(case family_planning_method when 160570 then "Emergency contraceptive pills" when 780 then "Oral Contraceptives Pills" when 5279 then "Injectible" when 1359 then "Implant"
   when 5275 then "Intrauterine Device" when 136163 then "Lactational Amenorhea Method" when 5278 then "Diaphram/Cervical Cap" when 5277 then "Fertility Awareness"
   when 1472 then "Tubal Ligation" when 190 then "Condoms" when 1489 then "Vasectomy" when 162332 then "Undecided" else "" end) as family_planning_method,
(case referred_from when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_from,
(case referred_to when 1537 then "Another Health Facility" when 163488 then "Community Unit" when 1175 then "N/A" else "" end) as referred_to,
 clinical_notes

from kenyaemr_etl.etl_mch_postnatal_visit;

ALTER TABLE kenyaemr_datatools.mch_postnatal_visit ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.mch_postnatal_visit ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.mch_postnatal_visit ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.mch_postnatal_visit ADD INDEX(arv_status);
ALTER TABLE kenyaemr_datatools.mch_postnatal_visit ADD INDEX(mother_hiv_status);
ALTER TABLE kenyaemr_datatools.mch_postnatal_visit ADD INDEX(arv_status);

SELECT "Successfully created mch_postnatal_visit table";

-- create table tb_enrollment
create table kenyaemr_datatools.tb_enrollment as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
location_id,
encounter_id,
date_treatment_started,
district,
(case referred_by when 160539 then "VCT center" when 160631 then "HIV care clinic" when 160546 then "STI Clinic" when 161359 then "Home Based Care"
when 160538 then "Antenatal/PMTCT Clinic" when 1725 then "Private Sector" when 1744 then "Chemist/pharmacist" when 160551 then "Self referral"
when 1555 then "Community Health worker(CHW)" when 162050 then "CCC" when 164103 then "Diabetes Clinic" else "" end) as referred_by,
referral_date,
date_transferred_in,
facility_transferred_from,
district_transferred_from,
date_first_enrolled_in_tb_care,
weight,
height,
treatment_supporter,
(case relation_to_patient when 973 then "Grandparent" when 972 then "Sibling" when 160639 then "Guardian" when 1527 then "Parent" when 5617 then "PARTNER OR SPOUSE"
 when 5622 then "Other" else "" end) as relation_to_patient,
treatment_supporter_address,
treatment_supporter_phone_contact,
(case disease_classification when 42 then "Pulmonary TB" when 5042 then "Extra-Pulmonary TB" else "" end) as disease_classification,
(case patient_classification when 159878 then "New" when 159877 then "Smear positive Relapse" when 159876 then "Smear negative Relapse" when 159874 then "Treatment after Failure"
when 159873 then "Treatment resumed after defaulting" when 159872 then "Transfer in" when 163609 then "Previous treatment history unknown"  else "" end) as patient_classification,
(case pulmonary_smear_result when 703 then "Smear Positive" when 664 then "Smear Negative" when 1118 then "Smear not done" else "" end) as pulmonary_smear_result,
(case has_extra_pulmonary_pleurial_effusion when 130059 then "Pleural effusion" else "" end) as has_extra_pulmonary_pleurial_effusion,
(case has_extra_pulmonary_milliary when 115753 then "Milliary" else "" end) as has_extra_pulmonary_milliary,
(case has_extra_pulmonary_lymph_node when 111953 then "Lymph nodes" else "" end) as has_extra_pulmonary_lymph_node,
(case has_extra_pulmonary_menengitis when 111967 then "Meningitis" else "" end) as has_extra_pulmonary_menengitis,
(case has_extra_pulmonary_skeleton when 112116 then "Skeleton" else "" end) as has_extra_pulmonary_skeleton,
(case has_extra_pulmonary_abdominal when 1350 then "Abdominal" else "" end) as has_extra_pulmonary_abdominal
from kenyaemr_etl.etl_tb_enrollment;

ALTER TABLE kenyaemr_datatools.tb_enrollment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.tb_enrollment ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.tb_enrollment ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.tb_enrollment ADD INDEX(disease_classification);
ALTER TABLE kenyaemr_datatools.tb_enrollment ADD INDEX(patient_classification);
ALTER TABLE kenyaemr_datatools.tb_enrollment ADD INDEX(pulmonary_smear_result);
ALTER TABLE kenyaemr_datatools.tb_enrollment ADD INDEX(date_first_enrolled_in_tb_care);
SELECT "Successfully created tb_enrollment table";

-- create table tb_follow_up_visit
create table kenyaemr_datatools.tb_follow_up_visit as
select
patient_id,
uuid,
provider,
visit_id,
visit_date ,
location_id,
encounter_id,
(case spatum_test when 160022 then "ZN Smear Microscopy" when 161880 then "Fluorescence Microscopy" else "" end) as spatum_test,
(case spatum_result when 159985 then "Scanty" when 1362 then "+" when 1363 then "++" when 1364 then "+++" when 664 then "Negative" else "" end) as spatum_result,
result_serial_number,
quantity ,
date_test_done,
(case bacterial_colonie_growth when 703 then "Growth" when 664 then "No growth" else "" end) as bacterial_colonie_growth,
number_of_colonies,
(case resistant_s when 84360 then "S" else "" end) as resistant_s,
(case resistant_r when 767 then "R" else "" end) as resistant_r,
(case resistant_inh when 78280 then "INH" else "" end) as resistant_inh,
(case resistant_e when 75948 then "E" else "" end) as resistant_e,
(case sensitive_s when 84360 then "S" else "" end) as sensitive_s,
(case sensitive_r when 767 then "R" else "" end) as sensitive_r,
(case sensitive_inh when 78280 then "INH" else "" end) as sensitive_inh,
(case sensitive_e when 75948 then "E" else "" end) as sensitive_e,
test_date,
(case hiv_status when 664 then "Negative" when 703 then "Positive" when 1067 then "Unknown" else "" end) as hiv_status,
next_appointment_date
from kenyaemr_etl.etl_tb_follow_up_visit;

ALTER TABLE kenyaemr_datatools.tb_follow_up_visit ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.tb_follow_up_visit ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.tb_follow_up_visit ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.tb_follow_up_visit ADD INDEX(hiv_status);

 SELECT "Successfully created tb_follow_up_visit";

-- create table tb_screening
create table kenyaemr_datatools.tb_screening as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
encounter_id,
location_id,
(case cough_for_2wks_or_more when 159799 then "Yes" when 1066 then "No" else "" end) as cough_for_2wks_or_more,
(case confirmed_tb_contact when 124068 then "Yes" when 1066 then "No" else "" end) as confirmed_tb_contact,
(case fever_for_2wks_or_more when 1494 then "Yes" when 1066 then "No" else "" end) as fever_for_2wks_or_more,
(case noticeable_weight_loss when 832 then "Yes" when 1066 then "No" else "" end) as noticeable_weight_loss,
(case night_sweat_for_2wks_or_more when 133027 then "Yes" when 1066 then "No" else "" end) as night_sweat_for_2wks_or_more,
(case resulting_tb_status when 1660 then "No TB Signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "TB Screening Not Done" else "" end) as resulting_tb_status,
tb_treatment_start_date,
notes
from kenyaemr_etl.etl_tb_screening;

ALTER TABLE kenyaemr_datatools.tb_screening ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.tb_screening ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.tb_screening ADD INDEX(encounter_id);

 SELECT "Successfully created tb_screening";

-- create table hei_enrollment
  create table kenyaemr_datatools.hei_enrollment as
    select
      serial_no,
      patient_id,
      uuid,
      provider,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      (case child_exposed when 822 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as child_exposed,
      spd_number,
      birth_weight,
      gestation_at_birth,
      date_first_seen,
      birth_notification_number,
      birth_certificate_number,
      (case need_for_special_care when 161628 then "Yes" when 1066 then "No" else "" end) as need_for_special_care,
      (case reason_for_special_care when 116222 then "Birth weight less than 2.5 kg" when 162071 then "Birth less than 2 years after last birth" when 162072 then "Fifth or more child" when 162073 then "Teenage mother"
       when 162074 then "Brother or sisters undernourished" when 162075 then "Multiple births(Twins,triplets)" when 162076 then "Child in family dead" when 1174 then "Orphan"
       when 161599 then "Child has disability" when 1859 then "Parent HIV positive" when 123174 then "History/signs of child abuse/neglect" else "" end) as reason_for_special_care,
      (case referral_source when 160537 then "Paediatric" when 160542 then "OPD" when 160456 then "Maternity" when 162050 then "CCC"  when 160538 then "MCH/PMTCT" when 5622 then "Other" else "" end) as referral_source,
      (case transfer_in when 1065 then "Yes" when 1066 then "No" else "" end) as transfer_in,
      transfer_in_date,
      facility_transferred_from,
      district_transferred_from,
      date_first_enrolled_in_hei_care,
      (case mother_breastfeeding when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as mother_breastfeeding,
      (case TB_contact_history_in_household when 1065 then "Yes" when 1066 then "No" else "" end) as TB_contact_history_in_household,
      (case mother_alive when 1 then "Yes" when 0 then "No" else "" end) as mother_alive,
      (case mother_on_pmtct_drugs when 1065 then "Yes" when 1066 then "No" else "" end) as mother_on_pmtct_drugs,
      (case mother_on_drug when 80586 then "Sd NVP Only" when 1652 then "AZT+NVP+3TC" when 1149 then "HAART" when 1107 then "None" else "" end) as mother_on_drug,
      (case mother_on_art_at_infant_enrollment when 1065 then "Yes" when 1066 then "No" else "" end) as mother_on_art_at_infant_enrollment,
      (case mother_drug_regimen when 792 then "D4T/3TC/NVP" when 160124 then "AZT/3TC/EFV" when 160104 then "D4T/3TC/EFV" when 1652 then "3TC/NVP/AZT"
       when 161361 then "EDF/3TC/EFV" when 104565 then "EFV/FTC/TDF" when 162201 then "3TC/LPV/TDF/r" when 817 then "ABC/3TC/AZT"
       when 162199 then "ABC/NVP/3TC" when 162200 then "3TC/ABC/LPV/r" when 162565 then "3TC/NVP/TDF" when 1652 then "3TC/NVP/AZT"
       when 162561 then "3TC/AZT/LPV/r" when 164511 then "AZT-3TC-ATV/r" when 164512 then "TDF-3TC-ATV/r" when 162560 then "3TC/D4T/LPV/r"
       when 162563 then "3TC/ABC/EFV" when 162562 then "ABC/LPV/R/TDF" when 162559 then "ABC/DDI/LPV/r"  else "" end) as mother_drug_regimen,
      (case infant_prophylaxis when 80586 then "Sd NVP Only" when 1652 then "sd NVP+AZT+3TC" when 1149 then "NVP for 6 weeks(Mother on HAART)" when 1107 then "None" else "" end) as infant_prophylaxis,
      parent_ccc_number,
      (case mode_of_delivery when 1170 then "SVD" when 1171 then "C-Section" else "" end) as mode_of_delivery,
      (case place_of_delivery when 1589 then "Facility" when 1536 then "Home" when 5622 then "Other" else "" end) as place_of_delivery,
      birth_length,
      birth_order,
      health_facility_name,
      date_of_birth_notification,
      date_of_birth_registration,
      birth_registration_place,
      permanent_registration_serial,
      mother_facility_registered

    from kenyaemr_etl.etl_hei_enrollment;

ALTER TABLE kenyaemr_datatools.hei_enrollment ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.hei_enrollment ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.hei_enrollment ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.hei_enrollment ADD INDEX(transfer_in);
ALTER TABLE kenyaemr_datatools.hei_enrollment ADD INDEX(child_exposed);
ALTER TABLE kenyaemr_datatools.hei_enrollment ADD INDEX(referral_source);
 SELECT "Successfully created hei_enrollment";

-- create table hei_follow_up_visit
 create table kenyaemr_datatools.hei_follow_up_visit as
select
patient_id,
uuid,
provider,
visit_id,
visit_date,
location_id,
encounter_id,
weight,
height,
(case primary_caregiver when 970 then "Mother" when 973 then "Guardian" when 972 then "Guardian" when 160639 then "Guardian" when 5622 then "Guardian" else "" end) as primary_caregiver,
(case infant_feeding when 5526 then "Exclusive Breastfeeding(EBF)" when 1595 then "Exclusive Replacement(ERF)" when 6046 then "Mixed Feeding(MF)" else "" end) as infant_feeding,
(case tb_assessment_outcome when 1660 then "No TB Signs" when 142177 then "Presumed TB" when 1661 then "TB Confirmed" when 1662 then "TB Rx" when 1679 then "INH" when 160737 then "TB Screening Not Done" else "" end) as tb_assessment_outcome,
(case social_smile_milestone when 162056 then "Social Smile" else "" end) as social_smile_milestone,
(case head_control_milestone when 162057 then "Head Holding/Control" else "" end) as head_control_milestone,
(case response_to_sound_milestone when 162058 then "Turns towards the origin of sound" else "" end) as response_to_sound_milestone,
(case hand_extension_milestone when 162059 then "Extends hand to grasp a toy" else "" end) as hand_extension_milestone,
(case sitting_milestone when 162061 then "Sitting" else "" end) as sitting_milestone,
(case walking_milestone when 162063 then "Walking" else "" end) as walking_milestone,
(case standing_milestone when 162062 then "Standing" else "" end) as standing_milestone,
(case talking_milestone when 162060 then "Talking" else "" end) as talking_milestone,
(case review_of_systems_developmental when 1115 then "Normal(N)" when 6022 then "Delayed(D)" when 6025 then "Regressed(R)" else "" end) as review_of_systems_developmental,
dna_pcr_sample_date,
(case dna_pcr_contextual_status when 162081 then "Repeat" when 162083 then "Final test (end of pediatric window)" when 162082 then "Confirmation" when 162080 then "Initial" else "" end) as dna_pcr_contextual_status,
(case dna_pcr_result when 1301 then "DETECTED" when 1302 then "NOT DETECTED" when 1300 then "EQUIVOCAL" when 1303 then "INHIBITORY" when 1304 then "POOR SAMPLE QUALITY" else "" end) as dna_pcr_result,
(case nvp_given when 80586 then "Yes" else "No" end) as nvp_given,
(case ctx_given when 105281 then "Yes" else "No" end) as ctx_given,
(case first_antibody_result when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as first_antibody_result,
(case final_antibody_result when 664 then "NEGATIVE" when 703 then "POSITIVE" when 1304 then "POOR SAMPLE QUALITY" else "" end) as final_antibody_result,
(case tetracycline_ointment_given  when 1065 then "Yes" when 1066 then "No" else "" end) as tetracycline_ointment_given,
(case pupil_examination when 162065 then "Black" when 1075 then "White" else "" end) as pupil_examination,
(case sight_examination when 1065 then "Following Objects" when 1066 then "Not Following Objects" else "" end) as sight_examination,
(case squint when 1065 then "Squint" when 1066 then "No Squint" else "" end) as squint,
(case deworming_drug when 79413 then "Mebendazole" when 70439 then "Albendazole" else "" end) as deworming_drug,
dosage,
unit,
comments,
next_appointment_date
from kenyaemr_etl.etl_hei_follow_up_visit;

ALTER TABLE kenyaemr_datatools.hei_follow_up_visit ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

ALTER TABLE kenyaemr_datatools.hei_follow_up_visit ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.hei_follow_up_visit ADD INDEX(encounter_id);
ALTER TABLE kenyaemr_datatools.hei_follow_up_visit ADD INDEX(infant_feeding);

 SELECT "Successfully created hei_follow_up_visit";

-- create table mch_delivery table
  create table kenyaemr_datatools.mch_delivery as
    select
      patient_id,
      uuid,
      provider,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      data_entry_date,
      duration_of_pregnancy,
      (case mode_of_delivery when 1170 then "Spontaneous vaginal delivery" when 1171 then "Cesarean section" when 1172 then "Breech delivery"
       when 118159 then "Forceps or Vacuum Extractor Delivery" when 159739 then "emergency caesarean section" when 159260 then "vacuum extractor delivery"
       when 5622 then "Other" when 1067 then "Unknown" else "" end) as mode_of_delivery,
      date_of_delivery,
      (case blood_loss when 1499 then "Moderate" when 1107 then "None" when 1498 then "Mild" when 1500 then "Severe" else "" end) as blood_loss,
      condition_of_mother,
      apgar_score_1min,
      apgar_score_5min,
      apgar_score_10min,
      (case resuscitation_done when 1065 then "Yes" when 1066 then "No" when 1067 then "Unknown" else "" end) as resuscitation_done,
      (case place_of_delivery when 1536 then "HOME" when 1588 then "HEALTH CLINIC/POST" when 1589 then "HOSPITAL"
       when 1601 then "EN ROUTE TO HEALTH FACILITY" when 159670 then "sub-district hospital" when 159671 then "Provincial hospital"
       when 159662 then "district hospital" when 159372 then "Primary Care Clinic" when 5622 then "Other" when 1067 then "Unknown" else "" end) as place_of_delivery,
      (case delivery_assistant when 1574 then "CLINICAL OFFICER/DOCTOR" when 1578 then "Midwife" when 1577 then "NURSE"
       when 1575 then "TRADITIONAL BIRTH ATTENDANT" when 1555 then "COMMUNITY HEALTH CARE WORKER" when 5622 then "Other" else "" end) as delivery_assistant,
      (case counseling_on_infant_feeding when 161651 then "Counseling about infant feeding practices" else "" end) as counseling_on_infant_feeding,
      (case counseling_on_exclusive_breastfeeding when 161096 then "Counseling for exclusive breastfeeding" else "" end) as counseling_on_exclusive_breastfeeding,
      (case counseling_on_infant_feeding_for_hiv_infected when 162091 then "Counseling for infant feeding practices to prevent HIV" else "" end) as counseling_on_infant_feeding_for_hiv_infected,
      (case mother_decision when 1173 then "EXPRESSED BREASTMILK" when 1152 then "WEANED" when 5254 then "Infant formula" when 1150 then "BREASTFED PREDOMINATELY"
       when 6046 then "Mixed feeding" when 5526 then "BREASTFED EXCLUSIVELY" when 968 then "COW MILK" when 1595 then "REPLACEMENT FEEDING"  else "" end) as mother_decision,
      (case placenta_complete when 163455 then "Complete placenta at delivery" when 163456 then "Incomplete placenta at delivery" else "" end) as placenta_complete,
      (case maternal_death_audited when 1065 then "Yes" when 1066 then "No" else "" end) as maternal_death_audited,
      (case cadre when 1574 then "CLINICAL OFFICER/DOCTOR" when 1578 then "Midwife" when 1577 then "NURSE" when 1575 then "TRADITIONAL BIRTH ATTENDANT" when 1555 then " COMMUNITY HEALTH CARE WORKER" when 5622 then "Other" else "" end) as cadre,
      other_delivery_complications,
      duration_of_labor,
      (case baby_sex when 1534 then "Male Gender" when 1535 then "Female gender" else "" end) as baby_sex,
      (case baby_condition when 135436 then "Macerated Stillbirth" when 159916 then "Fresh stillbirth" when 151849 then "Liveborn, Unspecified Whether Single, Twin, or Multiple "
       when 125872 then "STILLBIRTH" when 126127 then "Spontaneous abortion"
       when 164815 then "Live birth, died before arrival at facility"
       when 164816 then "Live birth, died after arrival or delivery in facility" else "" end) as baby_condition,
      (case teo_given when 84893 then "TETRACYCLINE" when 1066 then "No" when 1175 then "Not applicable" else "" end) as teo_given,
      birth_weight,
      (case bf_after_one_hour when 1065 then "Yes" when 1066 then "No" else "" end) as bf_after_one_hour,
      (case birth_with_deformity when 155871 then "deformity" when 1066 then "No"  when 1175 then "Not applicable" else "" end) as birth_with_deformity,
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
      partner_hiv_tested,
      partner_hiv_status,
      (case prophylaxis_given when 105281 then "SULFAMETHOXAZOLE / TRIMETHOPRIM" when 74250 then "DAPSONE"  when 1107 then "None" else "" end) as prophylaxis_given,
      (case haart_given_at_anc when 1 then "Yes" when 2 then "No" else "" end) as haart_given_at_anc,
      (case haart_given_at_delivery when 1 then "Yes" when 2 then "No" else "" end) as haart_given_at_delivery,
      haart_start_date,
      (case baby_azt_dispensed when 160123 then "Zidovudine for PMTCT" when 1066 then "No" when 1175 then "Not Applicable" else "" end) as baby_azt_dispensed,
      (case baby_nvp_dispensed when 80586 then "NEVIRAPINE" when 1066 then "No" when 1175 then "Not Applicable" else "" end) as baby_nvp_dispensed,
      clinical_notes

    from kenyaemr_etl.etl_mchs_delivery;

  ALTER TABLE kenyaemr_datatools.mch_delivery ADD FOREIGN KEY (patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

  ALTER TABLE kenyaemr_datatools.mch_delivery ADD INDEX(visit_date);
  ALTER TABLE kenyaemr_datatools.mch_delivery ADD INDEX(encounter_id);
  ALTER TABLE kenyaemr_datatools.mch_delivery ADD INDEX(final_test_result);
  ALTER TABLE kenyaemr_datatools.mch_delivery ADD INDEX(test_1_kit_name);
  ALTER TABLE kenyaemr_datatools.mch_delivery ADD INDEX(test_2_kit_name);
  ALTER TABLE kenyaemr_datatools.mch_delivery ADD INDEX(baby_sex);


SELECT "Creating HTS tables";

create table kenyaemr_datatools.hts_test as select * from kenyaemr_etl.etl_hts_test;
create table kenyaemr_datatools.hts_referral_and_linkage as select * from kenyaemr_etl.etl_hts_referral_and_linkage;

ALTER TABLE kenyaemr_datatools.hts_test ADD INDEX(visit_date);
ALTER TABLE kenyaemr_datatools.hts_test ADD FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.hts_test ADD index(visit_date);
ALTER TABLE kenyaemr_datatools.hts_test ADD index(population_type);
ALTER TABLE kenyaemr_datatools.hts_test ADD index(final_test_result);
ALTER TABLE kenyaemr_datatools.hts_test ADD index(test_1_kit_name);
ALTER TABLE kenyaemr_datatools.hts_test ADD index(test_2_kit_name);

ALTER TABLE kenyaemr_datatools.hts_referral_and_linkage ADD FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);
ALTER TABLE kenyaemr_datatools.hts_referral_and_linkage ADD index(visit_date);
ALTER TABLE kenyaemr_datatools.hts_referral_and_linkage ADD index(tracing_type);
ALTER TABLE kenyaemr_datatools.hts_referral_and_linkage ADD index(tracing_status);

SELECT "Creating current in care tables";
create table kenyaemr_datatools.current_in_care as select * from kenyaemr_etl.etl_current_in_care;
ALTER TABLE kenyaemr_datatools.current_in_care add FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

SELECT "Creating drug event table";
create table kenyaemr_datatools.drug_event as select * from kenyaemr_etl.etl_drug_event;
alter table kenyaemr_datatools.drug_event add FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

SELECT "Creating IPT screening table";
create table kenyaemr_datatools.ipt_screening as select * from kenyaemr_etl.etl_ipt_screening;
alter table kenyaemr_datatools.ipt_screening add FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

SELECT "Creating IPT followup table";
create table kenyaemr_datatools.ipt_followup as select * from kenyaemr_etl.etl_ipt_follow_up;
alter table kenyaemr_datatools.ipt_followup add FOREIGN KEY(patient_id) REFERENCES kenyaemr_datatools.patient_demographics(patient_id);

SELECT "Completed data tool tables";
END$$
DELIMITER ;
