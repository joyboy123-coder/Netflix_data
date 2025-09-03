import pandas as pd
import logging
import re
import random


logging.basicConfig(
    filename="transform.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logging.info("DATA CLEANING AND  TRANSFORMATIONS STARTED\n")

        df.columns = df.columns.str.upper()
        logging.info("Standardized column names to uppercase")

        df['TITLE'] = (df['TITLE']
            .astype(str)
            .str.strip()
            .str.replace('0','o')
            .str.replace('1','i')
            .str.title()
            .str.replace(r'[^a-zA-Z\s]','',regex=True)
            .str.replace(r'\s+',' ',regex=True)
        )
        logging.info("Cleaned TITLE column")

        # --- Clean TYPE ---
        df['TYPE'] = df['TYPE'].str.title().str.strip().str.replace('Tv','TV')

        # --- Release Year ---
        df['RELEASE_YEAR'] = pd.to_datetime(df['RELEASE_YEAR'], format='%Y', errors='coerce')
        df['RELEASE_YEAR'] = df['RELEASE_YEAR'].dt.year
        logging.info("Converted RELEASE_YEAR to numeric year")

        # --- Genre ---
        df['GENRE'] = (df['GENRE']
            .astype(str)
            .str.replace(r'[^a-zA-Z\s]','',regex=True)
            .str.title()
            .str.strip()
        )

        genre_typo = {
            "Hrrr": "Horror",
            "Anmatn": "Animation",
            "Cmedy": "Comedy",
            "Thrller": "Thriller",
            "Actn": "Action",
            "Scf": "Sci-Fi",
            "Dcumentary": "Documentary",
            "Scifi": "Sci-Fi"
        }
        df['GENRE'] = df['GENRE'].replace(genre_typo)

        # Fill missing GENRE with random choice
        df['GENRE'] = df['GENRE'].apply(lambda x: random.choice(["Sci-Fi","Comedy","Action","Thriller","Horror"]) if pd.isna(x) or x=='' else x)
        logging.info("Cleaned GENRE column and filled missing values")

        # --- Duration ---
        def convert_duration(x):
            if pd.isna(x): return None
            t = str(x).lower().strip()

            if m := re.match(r"(\d+)h\s*(\d+)m", t): return int(m[1])*60+int(m[2])
            if m := re.match(r"(\d+)h$", t): return int(m[1])*60
            if m := re.match(r"(\d+)\s*min", t): return int(m[1])
            if m := re.match(r"(\d+)\s*episodes?", t): return int(m[1])*30
            if m := re.match(r"(\d+)\s*seasons?", t): return int(m[1])*120
            return None

        df["DURATION_MINUTES"] = df["DURATION"].map(convert_duration)
        df.drop(columns='DURATION', inplace=True)
        logging.info("Converted DURATION to DURATION_MINUTES")

        # --- Country ---
        df['COUNTRY'] = (df['COUNTRY']
            .astype(str)
            .str.replace(r'[^a-zA-Z\s]','',regex=True)
            .str.title()
            .str.strip()
        )

        country_typo = {
            "Suth Krea": "South Korea",
            "Unted Kngdm": "United Kingdom",
            "Unted States": "United States",
            "Brazl": "Brazil",
            "Australa": "Australia",
            "Inda": "India"
        }
        df['COUNTRY'] = df['COUNTRY'].replace(country_typo)
        logging.info("Cleaned COUNTRY column")

        # --- Rating ---
        df['RATING'] = (df['RATING']
            .astype(str)
            .str.replace(r'[^a-zA-Z0-9\s]','',regex=True)
            .str.strip()
            .str.upper()
        )

        rating_typo = {
            "PG13": "PG-13",
            "TVPG": "TV-PG",
            "TV14": "TV-14",
            "TVMA": "TV-MA",
            "TVY7": "TV-Y7",
            "R": "R",
            "G": "G",
            "PG": "PG"
        }
        df['RATING'] = df['RATING'].replace(rating_typo)
        logging.info("Standardized RATING column")

        df['IMDB_SCORE'] = pd.to_numeric(df['IMDB_SCORE'], errors='coerce')

        df['ADDED_DATE'] = pd.to_datetime(df['ADDED_DATE'], errors='coerce')

        logging.info("Finished TRANSFORM process successfully")
        return df

    except Exception as e:
        logging.error(f"Transformation failed: {e}\n")
        return df
    
    finally:
        logging.info("DATA CLEANING DONE......\n")
        logging.info("---------------------------------------------------")