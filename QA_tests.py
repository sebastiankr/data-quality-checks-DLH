

def get_tests():    # write Fibonacci series up to n
    return [
        {
            "source":"CIIS",
            "test_name": "Females",
            "source_query":
"""
SELECT  pc.patient_id as record_source_person_id, pc.gender_code as current_gender
FROM `ciis_replica.dbo_patients_core` pc
WHERE pc.gender_code='F'
ORDER BY pc.patient_id
--LIMIT 10
""",
            "target_query":
"""
SELECT sppm.record_source_person_id, sppm.current_gender
FROM `raw_vault.s_person_profile_main`  sppm
where record_source = 'ciis' AND sppm.current_gender = 'Female' AND sppm.effective_end_dt IS NULL
ORDER BY sppm.record_source_person_id
--LIMIT 10
""",
            "notes": ""
        },   
        
        
           {
            "source":"EpiTrax",
            "test_name": "Person addresses",
            "source_query":
"""
select 
--count(*)
p.id as record_source_person_id, pa.id as record_source_person_address_id
from `epitrax_replica.public_person` p
JOIN `epitrax_replica.public_person_address` pa ON pa.person_id = p.id
ORDER BY p.id, pa.id
--LIMIT 10
""",
            "target_query":
"""
SELECT 
--count(*)
sppm.record_source_person_id, spa.record_source_person_address_id
FROM `raw_vault.s_person_profile_main` sppm 
JOIN `raw_vault.s_person_address` spa ON spa.h_person_sid = sppm.h_person_sid AND sppm.record_owner = spa.record_owner
WHERE 
sppm.effective_end_dt IS NULL AND spa.effective_end_dt IS NULL AND sppm.record_source = 'epitrax'
GROUP BY sppm.record_source_person_id, spa.record_source_person_address_id
ORDER BY sppm.record_source_person_id, spa.record_source_person_address_id
--LIMIT 10
""",
            "notes": ""
        },      
        
        {
            "source":"CIIS",
            "test_name": "Persons with CVX code 8",
            "source_query":
"""
SELECT 
pc.patient_id as record_source_person_id, vaccination_date 
FROM `ciis_replica.dbo_patients_core` pc
JOIN `ciis_replica.dbo_patient_vaccinations` pv ON pv.patient_id = pc.patient_id
WHERE vaccination_code_id IN(8)
ORDER BY pc.patient_id, pv.vaccination_date
--LIMIT 10
""",
            "target_query":
"""
SELECT 
--count(*)
sppm.record_source_person_id, vaccination_date
FROM `raw_vault.s_person_profile_main` sppm 
JOIN `raw_vault.l_person_vaccination` lpv ON lpv.h_person_sid = sppm.h_person_sid AND sppm.record_owner = lpv.record_owner
JOIN `raw_vault.s_vaccination_main` svm ON svm.h_vaccination_sid = lpv.h_vaccination_sid AND lpv.record_owner = svm.record_owner
WHERE 
sppm.effective_end_dt IS NULL AND sppm.record_source = 'ciis'
AND svm.effective_end_dt IS NULL
AND sppm.record_owner = 'Immunization Branch'
AND svm.cvx_code IN(8)
ORDER BY sppm.record_source_person_id, vaccination_date
--LIMIT 10
""",
            "notes": ""
        }, 
        
        {
            "source":"EpiTrax",
            "test_name": "OSHV person facility visits per condition",
            "source_query":
"""
select 
--count(*)
p.id as record_source_person_id, 
from `epitrax_replica.public_person` p
JOIN `epitrax_replica.public_person_condition` pc ON pc.person_id = p.id
JOIN `epitrax_replica.public_condition` c ON c.id = pc.condition_id
JOIN `epitrax_replica.public_person_condition_person_facility` pcpf ON pcpf.person_condition_id = pc.id
JOIN `epitrax_replica.public_person_facility` pf ON pcpf.person_facility_id = pf.id
WHERE c.id IN (41,44,75,96,95,97,121,176,177,178,216,179,180,181,182,86,87,89,85,88,90,91,93)
ORDER BY p.id
--LIMIT 10
""",
            "target_query":
"""
SELECT 
--count(*)
sppm.record_source_person_id
FROM `raw_vault.s_person_profile_main` sppm 
JOIN `raw_vault.l_person_condition` lpc ON lpc.h_person_sid = sppm.h_person_sid AND lpc.record_owner = sppm.record_owner
JOIN `raw_vault.s_person_condition` spc ON spc.h_person_condition_sid = lpc.h_person_condition_sid AND spc.record_owner = lpc.record_owner
JOIN `raw_vault.s_condition_main` scm ON scm.h_condition_sid = lpc.h_condition_sid AND scm.record_owner = lpc.record_owner

JOIN `raw_vault.l_person_condition_facility_visit` pcfv ON pcfv.h_person_condition_sid = lpc.h_person_condition_sid AND pcfv.record_owner = lpc.record_owner
JOIN `raw_vault.s_facility_visit` sfv ON sfv.h_facility_visit_sid = pcfv.h_facility_visit_sid AND sfv.record_owner = pcfv.record_owner


WHERE scm.record_source_condition_id IN (41,44,75,96,95,97,121,176,177,178,216,179,180,181,182,86,87,89,85,88,90,91,93)
AND sppm.effective_end_dt IS NULL AND spc.effective_end_dt IS NULL AND scm.effective_end_dt IS NULL AND sfv.effective_end_dt IS NULL
ORDER BY sppm.record_source_person_id
--LIMIT 10
""",
            "notes": ""
        },       
        
        
        {
            "source":"EpiTrax",
            "test_name": "CDB conditions",
            "source_query":
"""
select 
--count(*)
p.id as record_source_person_id, 
from `epitrax_replica.public_person` p
JOIN `epitrax_replica.public_person_condition` pc ON pc.person_id = p.id
JOIN `epitrax_replica.public_condition` c ON c.id = pc.condition_id
WHERE c.id NOT IN (115, 129,   41,44,75,96,95,97,121,176,177,178,216,179,180,181,182,86,87,89,85,88,90,91,93)
ORDER BY p.id
--LIMIT 50
""",
            "target_query":
"""
SELECT 
--count(*)
sppm.record_source_person_id
FROM `raw_vault.s_person_profile_main` sppm 
JOIN `raw_vault.l_person_condition` lpc ON lpc.h_person_sid = sppm.h_person_sid AND lpc.record_owner = sppm.record_owner
JOIN `raw_vault.s_person_condition` spc ON spc.h_person_condition_sid = lpc.h_person_condition_sid AND spc.record_owner = lpc.record_owner
JOIN `raw_vault.s_condition_main` scm ON scm.h_condition_sid = lpc.h_condition_sid AND scm.record_owner = lpc.record_owner
WHERE scm.record_source_condition_id NOT IN (115, 129,  41,44,75,96,95,97,121,176,177,178,216,179,180,181,182,86,87,89,85,88,90,91,93)
AND sppm.effective_end_dt IS NULL AND spc.effective_end_dt IS NULL AND scm.effective_end_dt IS NULL
ORDER BY sppm.record_source_person_id
--LIMIT 50
""",
            "notes": ""
        },
        
        {
            "source":"EpiTrax",
            "test_name": "OSHV conditions",
            "source_query":
"""
select 
--count(*)
p.id as record_source_person_id, 
from `epitrax_replica.public_person` p
JOIN `epitrax_replica.public_person_condition` pc ON pc.person_id = p.id
JOIN `epitrax_replica.public_condition` c ON c.id = pc.condition_id
WHERE c.id IN (41,44,75,96,95,97,121,176,177,178,216,179,180,181,182,86,87,89,85,88,90,91,93)
ORDER BY p.id
--LIMIT 50
""",
            "target_query":
"""
SELECT 
--count(*)
sppm.record_source_person_id
FROM `raw_vault.s_person_profile_main` sppm 
JOIN `raw_vault.l_person_condition` lpc ON lpc.h_person_sid = sppm.h_person_sid AND lpc.record_owner = sppm.record_owner
JOIN `raw_vault.s_person_condition` spc ON spc.h_person_condition_sid = lpc.h_person_condition_sid AND spc.record_owner = lpc.record_owner
JOIN `raw_vault.s_condition_main` scm ON scm.h_condition_sid = lpc.h_condition_sid AND scm.record_owner = lpc.record_owner
WHERE scm.record_source_condition_id IN (41,44,75,96,95,97,121,176,177,178,216,179,180,181,182,86,87,89,85,88,90,91,93)
AND sppm.effective_end_dt IS NULL AND spc.effective_end_dt IS NULL AND scm.effective_end_dt IS NULL
ORDER BY sppm.record_source_person_id
--LIMIT 50
""",
            "notes": ""
        },
        
        {
            "source":"EpiTrax",
            "test_name": "DEHS conditions",
            "source_query":
"""
select 
--count(*)
p.id as record_source_person_id, 
from `epitrax_replica.public_person` p
JOIN `epitrax_replica.public_person_condition` pc ON pc.person_id = p.id
JOIN `epitrax_replica.public_condition` c ON c.id = pc.condition_id
WHERE c.id IN (115, 129)
ORDER BY p.id
--LIMIT 50
""",
            "target_query":
"""
SELECT 
--count(*)
sppm.record_source_person_id
FROM `raw_vault.s_person_profile_main` sppm 
JOIN `raw_vault.l_person_condition` lpc ON lpc.h_person_sid = sppm.h_person_sid AND lpc.record_owner = sppm.record_owner
JOIN `raw_vault.s_person_condition` spc ON spc.h_person_condition_sid = lpc.h_person_condition_sid AND spc.record_owner = lpc.record_owner
JOIN `raw_vault.s_condition_main` scm ON scm.h_condition_sid = lpc.h_condition_sid AND scm.record_owner = lpc.record_owner
WHERE scm.record_source_condition_id IN (115, 129)
AND sppm.effective_end_dt IS NULL AND spc.effective_end_dt IS NULL AND scm.effective_end_dt IS NULL
ORDER BY sppm.record_source_person_id
--LIMIT 50
""",
            "notes": ""
        },
        
         {
            "source":"EpiTrax",
            "test_name": "all labs and tests for person conditions",
            "source_query":
"""
SELECT 
--count(*)
pc.id as record_source_person_condition_id, pc.group_living,
l.id as record_source_lab_id, l.collection_date,
pr.first_name as provider_first_name,
lt.id as record_source_labtest_id,lt.loinc_code,
ltr.test_result,
lts.code as test_status
FROM `epitrax_replica.public_person_condition` pc
JOIN `epitrax_replica.public_person_condition_lab` pcl ON pcl.person_condition_id = pc.id
JOIN `epitrax_replica.public_lab` l ON l.id = pcl.lab_id
LEFT JOIN `epitrax_replica.public_provider` pr ON l.provider_id = pr.id
LEFT JOIN `epitrax_replica.public_lab_test` lt ON l.id = lt.lab_id
LEFT JOIN `epitrax_replica.public_lab_test_result` ltr ON lt.id = ltr.lab_test_id
LEFT JOIN `epitrax_replica.public_lab_test_status` lts ON lts.id = lt.test_status_id
ORDER BY pc.id,l.id, lt.id
--LIMIT 10
""",
            "target_query":
"""
SELECT 
spc.record_source_person_condition_id, spc.group_living,
sls.record_source_lab_id, sls.collection_date, sls.provider_first_name,
slt.record_source_labtest_id, slt.loinc_code, slt.test_result, slt.test_status
FROM `raw_vault.s_person_condition` spc 
JOIN `raw_vault.l_person_condition_lab` lpcl ON lpcl.h_person_condition_sid = spc.h_person_condition_sid AND lpcl.record_owner = spc.record_owner
LEFT JOIN `raw_vault.s_lab_main` sls ON lpcl.h_lab_sid = sls.h_lab_sid AND sls.record_owner = spc.record_owner
LEFT JOIN `raw_vault.s_lab_labtest` slt ON lpcl.h_labtest_sid = slt.h_labtest_sid AND slt.record_owner = spc.record_owner
WHERE spc.effective_end_dt IS NULL AND sls.effective_end_dt IS NULL AND slt.effective_end_dt IS NULL
GROUP BY spc.record_source_person_condition_id, spc.group_living, sls.record_source_lab_id, sls.collection_date, sls.provider_first_name, slt.record_source_labtest_id,slt.loinc_code, slt.test_result, slt.test_status
ORDER BY spc.record_source_person_condition_id, sls.record_source_lab_id, slt.record_source_labtest_id
--LIMIT 50
""",
            "notes": "labs without tests cause problems, link needs nullable tests"
        },
        {
            "source":"EpiTrax",
            "test_name": "all vaccinations",
            "source_query":
"""
select 
--count(*)
p.id as record_source_person_id, CAST(p.date_of_death AS DATE) as death_date, 
pv.administered_date as vaccination_date, pv.dose_number as dosage_number, pv.lot_number, pv.national_drug_code as ndc_number, pv.vaccination_record_identifier, pv.expiration_date as vaccination_expiration_dt,
v.code as cvx_desc
FROM `epitrax_replica.public_person` p
JOIN `epitrax_replica.public_person_vaccine` pv ON pv.person_id = p.id
LEFT JOIN `epitrax_replica.public_vaccine` v ON v.id = pv.vaccine_id
ORDER BY p.id,pv.id
--LIMIT 10
""",
            "target_query":
"""
SELECT 
--count(*)
--sppm.record_owner, sppm.record_source,
sppm.record_source_person_id, sppm.death_date,
svm.vaccination_date, svm.dosage_number, svm.lot_number, svm.ndc_number, svm.vaccination_record_identifier, svm.vaccination_expiration_dt, cvx_desc
--, svm.data_source, svm.comment,
--, sppm.birth_date, spc.condition_name, scm.cdc_code, spc.date_diagnosed, spc.outbreak_name, spc.outbreak_associated
FROM `raw_vault.s_person_profile_main` sppm 
JOIN `raw_vault.l_person_vaccination` lpv ON lpv.h_person_sid = sppm.h_person_sid AND sppm.record_owner = lpv.record_owner
JOIN `raw_vault.s_vaccination_main` svm ON svm.h_vaccination_sid = lpv.h_vaccination_sid AND lpv.record_owner = svm.record_owner
WHERE 
sppm.effective_end_dt IS NULL AND sppm.record_source = 'epitrax'
AND svm.effective_end_dt IS NULL
AND lpv.record_source = 'epitrax'
AND svm.record_source = 'epitrax'
AND sppm.record_owner = 'CDB' 
ORDER BY sppm.record_source_person_id, vaccination_date
--LIMIT 10
""",
            "notes": "- cvx_desc and cvx_code?"
        },

        {
            "source":"EpiTrax",
            "test_name": "COVID_conditions",
            "source_query":
"""
select 
--count(*)
first_name, last_name, 
CASE birth_gender WHEN 'F' THEN 'Female' WHEN 'M' THEN 'Male' WHEN 'UNK' THEN 'Unknown' ELSE 'None' END AS birth_gender, p.birth_date, c.name as condition_name, c.cdc_code, pc.date_diagnosed, o.name as outbreak_name, pc.outbreak_associated
from `epitrax_replica.public_person` p
JOIN `epitrax_replica.public_person_condition` pc ON pc.person_id = p.id
JOIN `epitrax_replica.public_condition` c ON c.id = pc.condition_id
LEFT JOIN `epitrax_replica.public_outbreak` o ON o.id = pc.outbreak_id
WHERE c.name = 'COVID-19'
ORDER BY p.birth_date, p.first_name, p.last_name
--LIMIT 10
""",
            "target_query":
"""
SELECT 
--count(*)
sppm.first_name, sppm.last_name, sppm.birth_gender, sppm.birth_date, spc.condition_name, scm.cdc_code, spc.date_diagnosed, spc.outbreak_name, spc.outbreak_associated
FROM `raw_vault.s_person_profile_main` sppm 
JOIN `raw_vault.l_person_condition` lpc ON lpc.h_person_sid = sppm.h_person_sid
JOIN `raw_vault.s_person_condition` spc ON spc.h_person_condition_sid = lpc.h_person_condition_sid
JOIN `raw_vault.s_condition_main` scm ON scm.h_condition_sid = lpc.h_condition_sid
WHERE scm.condition_name = 'COVID-19'
AND sppm.record_owner = 'CDB' AND spc.record_owner = 'CDB'
AND sppm.effective_end_dt IS NULL AND spc.effective_end_dt IS NULL AND scm.effective_end_dt IS NULL
ORDER BY sppm.birth_date, sppm.first_name, sppm.last_name
--LIMIT 10
""",
            "notes": "- exposure_date missing"
        }
    ]




#                 {
#             "source":"EpiTrax",
#             "test_name": "lab_tests per person",
#             "source_query":
# """
# SELECT 
# --count(*)
# p.id as record_source_person_id, p.country_of_birth as birth_country,
# l.id as record_source_lab_id, l.collection_date,
# pr.first_name as provider_first_name,
# lt.loinc_code,
# ltr.test_result,
# lts.code as test_status
# FROM `epitrax_replica.public_person` p
# JOIN `epitrax_replica.public_lab` l ON l.person_id = p.id
# LEFT JOIN `epitrax_replica.public_provider` pr ON l.provider_id = pr.id
# LEFT JOIN `epitrax_replica.public_lab_test` lt ON l.id = lt.lab_id
# LEFT JOIN `epitrax_replica.public_lab_test_result` ltr ON lt.id = ltr.lab_test_id
# LEFT JOIN `epitrax_replica.public_lab_test_status` lts ON lts.id = lt.test_status_id
# ORDER BY p.id,l.id
# --LIMIT 10
# """,
#             "target_query":
# """
# SELECT 
# --count(*)
# sppm.record_source_person_id, sppm.birth_country, 
# sls.record_source_lab_id, sls.collection_date,sls.provider_first_name,
# slt.loinc_code, slt.test_result, slt.test_status
# FROM `raw_vault.s_person_profile_main` sppm 
# JOIN `raw_vault.l_person_lab` lpl ON lpl.h_person_sid = sppm.h_person_sid
# LEFT JOIN `raw_vault.s_lab_specimen` sls ON lpl.h_lab_sid = sls.h_lab_sid
# LEFT JOIN `raw_vault.s_lab_labtest` slt ON lpl.h_labtest_sid = slt.h_labtest_sid
# WHERE sppm.record_source = 'epitrax'
# AND sppm.effective_end_dt IS NULL
# AND sls.effective_end_dt IS NULL
# ORDER BY sppm.record_source_person_id
# --LIMIT 10
# """,
#             "notes": ""
#         },


#         {
#             "source":"EpiTrax",
#             "test_name": "s_condition_main",
#             "source_query":
# """
# SELECT
#  c.id as record_source_condition_id, c.name as condition_name, contact_lead_in, place_lead_in, treatment_lead_in, active, cdc_code, code as condition_code, mdro 
# FROM `epitrax_replica.public_condition` c 
# JOIN `epitrax_replica.public_user` u ON u.id = c.coordinator_id ORDER BY c.id;
# """,
#             "target_query":
# """
# SELECT 
#  record_source_condition_id, condition_name, contact_lead_in, place_lead_in, treatment_lead_in,active, cdc_code, condition_code, mdro
# FROM raw_vault.s_condition_main 
# WHERE record_source = 'epitrax' AND effective_end_dt IS NULL
# ORDER BY record_source_condition_id;
# """,
#             "notes": "wrong end dates, coordinator name concatenated wrongly, leading to , in name"
#         },
