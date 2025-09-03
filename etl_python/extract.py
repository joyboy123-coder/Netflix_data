import pandas as pd
import logging


logging.basicConfig(
    filename="log_file.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract(raw_data):
    try:
        logging.info("-------------------------------------------")
        logging.info("EXTRACTING THE DATA\n")

        logging.info(f"Started Extracting Data from {raw_data}\n")

        df = pd.read_csv(raw_data)

        if df.empty:
            logging.warning(f"Extracted DataFrame is empty from {raw_data}\n")
        
        else:
            logging.info(f"Successfully Extracted {len(df)} rows from {raw_data}\n")

        
        return df
    
    except Exception as e:
        logging.error(f"Exception Failed : {e}\n")
        return pd.DataFrame()
    
    finally:
        logging.info("---------------------------------------------")