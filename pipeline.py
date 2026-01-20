import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# KoboToolbox credentials
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/a3zYq3MQmDfpmYaFfWZQ9n/export-settings/esDuEQFP5kQtPt2oMxwsh2y/data.csv"

# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

# Schema and table details
schema_name = "Ribka-Blossom-academy-capstone-project"
table_name = "public_health_data"

# ==================== STEP 1: FETCH DATA FROM KOBOTOOLBOX ====================
print("📥 Fetching data from KoboToolbox...")
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code == 200:
    print("✅ Data fetched successfully from KoboToolbox")
    
    # Read CSV data into pandas DataFrame
    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip')
    
    print(f"📊 Fetched {len(df)} records")
    print(f"📋 Columns: {len(df.columns)} columns found")
    
    # ==================== STEP 2: CLEAN AND TRANSFORM DATA ====================
    print("\n🔄 Processing and transforming data...")
    
    # Clean column names - remove spaces, special characters
    df.columns = [col.strip().replace(" ", "_").replace("/", "_").replace("?", "").replace("-", "_").replace("&", "and") for col in df.columns]
    
    # Convert date columns to proper datetime format
    if 'start' in df.columns:
        df['start'] = pd.to_datetime(df['start'], errors='coerce')
    if 'end' in df.columns:
        df['end'] = pd.to_datetime(df['end'], errors='coerce')
    
    # NO DUPLICATE REMOVAL - Keep all records
    print(f"✅ Data cleaned successfully - {len(df)} records ready for upload (keeping all records)")
    
    # ==================== STEP 3: UPLOAD TO POSTGRESQL ====================
    print("\n📤 Connecting to PostgreSQL...")
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT
        )
        
        cur = conn.cursor()
        print("✅ Connected to PostgreSQL")
        
        # Create schema if it doesn't exist
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
        print(f"✅ Schema '{schema_name}' ready")
        
        # Drop and recreate table (fresh start each time)
        cur.execute(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}";')
        
        # Find the actual column names for our questions
        age_col = [c for c in df.columns if 'age' in c.lower() and 'group' in c.lower()]
        vacc_col = [c for c in df.columns if 'vaccin' in c.lower()]
        visit_col = [c for c in df.columns if 'healthcare' in c.lower() or 'visit' in c.lower()]
        exercise_col = [c for c in df.columns if 'exercise' in c.lower() or 'physical' in c.lower()]
        water_col = [c for c in df.columns if 'water' in c.lower() or 'drinking' in c.lower()]
        sleep_col = [c for c in df.columns if 'sleep' in c.lower() or 'hour' in c.lower()]
        insurance_col = [c for c in df.columns if 'insurance' in c.lower() or 'coverage' in c.lower()]
        gender_col = [c for c in df.columns if 'gender' in c.lower()]
        
        # Create table with appropriate columns for public health survey
        cur.execute(f"""
            CREATE TABLE "{schema_name}"."{table_name}" (
                id SERIAL PRIMARY KEY,
                submission_start TIMESTAMP,
                submission_end TIMESTAMP,
                age_group VARCHAR(100),
                gender VARCHAR(50),
                vaccination_status VARCHAR(100),
                healthcare_visits_count INTEGER,
                exercise_frequency VARCHAR(100),
                water_source VARCHAR(100),
                sleep_hours DECIMAL(5,2),
                health_insurance VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print(f"✅ Table '{table_name}' created")
        
        # Prepare insert query
        insert_query = f"""
            INSERT INTO "{schema_name}"."{table_name}" (
                submission_start, submission_end, age_group, gender, vaccination_status,
                healthcare_visits_count, exercise_frequency, water_source,
                sleep_hours, health_insurance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Insert data row by row
        records_inserted = 0
        errors_count = 0
        
        print(f"\n📥 Inserting {len(df)} records...")
        
        for idx, row in df.iterrows():
            try:
                # Get values using the actual column names we found
                age_val = row.get(age_col[0]) if age_col else None
                gender_val = row.get(gender_col[0]) if gender_col else None
                vacc_val = row.get(vacc_col[0]) if vacc_col else None
                visit_val = row.get(visit_col[0], 0) if visit_col else 0
                exercise_val = row.get(exercise_col[0]) if exercise_col else None
                water_val = row.get(water_col[0]) if water_col else None
                sleep_val = row.get(sleep_col[0], 0) if sleep_col else 0
                insurance_val = row.get(insurance_col[0]) if insurance_col else None
                
                # Convert numeric values
                try:
                    visit_val = int(float(visit_val)) if pd.notna(visit_val) else 0
                except:
                    visit_val = 0
                
                try:
                    sleep_val = float(sleep_val) if pd.notna(sleep_val) else 0
                except:
                    sleep_val = 0
                
                cur.execute(insert_query, (
                    row.get('start'),
                    row.get('end'),
                    age_val,
                    gender_val,
                    vacc_val,
                    visit_val,
                    exercise_val,
                    water_val,
                    sleep_val,
                    insurance_val
                ))
                records_inserted += 1
                
                # Show progress every 10 records
                if records_inserted % 10 == 0:
                    print(f"   Inserted {records_inserted}/{len(df)} records...")
                
            except Exception as e:
                errors_count += 1
                if errors_count <= 3:  # Only print first 3 errors
                    print(f"⚠️  Warning: Could not insert row {idx}: {e}")
        
        # Commit changes
        conn.commit()
        print(f"\n✅ Successfully inserted {records_inserted} records into PostgreSQL!")
        if errors_count > 0:
            print(f"⚠️  {errors_count} records failed to insert")
        
        # Verify the data
        cur.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}";')
        total_records = cur.fetchone()[0]
        print(f"📊 Total records in database: {total_records}")
        
        # Close connections
        cur.close()
        conn.close()
        print("🔌 Database connection closed")
        
        print("\n" + "="*60)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except psycopg2.Error as e:
        print(f"\n❌ PostgreSQL Error: {e}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    
else:
    print(f"❌ Failed to fetch data from KoboToolbox. Status code: {response.status_code}")
    print("Please check:")
    print("  1. Your KoboToolbox username and password in .env file")
    print("  2. Your KOBO_CSV_URL is correct")
    print("  3. You have internet connection")