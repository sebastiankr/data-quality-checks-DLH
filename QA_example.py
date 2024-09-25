def get_tests():    # write Fibonacci series up to n
    return [       {
            "source":"EpiTrax",
            "test_name": "Person addresses",
            "source_query":
"""
select 
--count(*)
p.id as record_source_person_id, pa.id as record_source_person_address_id
from `epitrax_replica.public_person` p
JOIN `epitrax_replica.public_person_address` pa ON pa.person_id = p.id
where DATE(created_at) between '{start_time}' and  '{end_time}' 
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
AND DATE(spa.load_dt) between '{start_time}' and  '{end_time}'  AND
 DATE(sppm.load_dt) between '{start_time}' and  '{end_time}' 
GROUP BY sppm.record_source_person_id, spa.record_source_person_address_id
ORDER BY sppm.record_source_person_id, spa.record_source_person_address_id
--LIMIT 10
""",
            "notes": ""
        }]