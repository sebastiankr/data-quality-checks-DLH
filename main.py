import subprocess
import logging
import concurrent.futures

logging.basicConfig(filename='script_runner.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

def run_script(script_name):
    try:
        logging.info(f"starting {script_name}")
        result=subprocess.run(['python',script_name],check=True)
        logging.info(f"{script_name} ran successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_name}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error running {script_name} : {e}")
if __name__=='__main__':
    scripts = [
        'hashid_check.py',
        'model_checks.py',
        'date_checks.py',
        'biz_vault_UatVsProd.py',
        'bizvault_view_definition_comparision.py',
        'schema_UatvsProd.py',
        'QA_time.py']
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures=[]

        for script in scripts:
            futures.append(executor.submit(run_script,script))
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e :
                logging.error(f"Error during execution: {e}")
    logging.info("All scripts have finished executing")