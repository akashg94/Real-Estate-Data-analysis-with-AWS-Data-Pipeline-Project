import json
import boto3
import urllib.request
import urllib.parse
import time
from datetime import datetime

s3_client = boto3.client('s3')

# Census API Key
CENSUS_API_KEY = "CENSUS_API_KEY"

# S3 Configuration
OUTPUT_BUCKET = "kaggle-realestate-pipeline-raw-group4"
OUTPUT_KEY = "census/census_multistate_data.json"  

def fetch_census_data_for_zip(zip_code, state_code):
    """Fetch demographic data for a ZIP code from Census Bureau API"""
    
    base_url = "https://api.census.gov/data/2021/acs/acs5"
    
    variables = [
        "NAME", "B19013_001E", "B01003_001E", "B15003_022E",
        "B15003_023E", "B15003_024E", "B15003_025E", "B15003_001E",
        "B23025_005E", "B23025_002E", "B01002_001E"
    ]
    
    params = {
        "get": ",".join(variables),
        "for": f"zip code tabulation area:{zip_code}",
        "key": CENSUS_API_KEY
    }
    
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        if len(data) < 2:
            return None
        
        values = data[1]
        
        def safe_int(val):
            try:
                num = int(val)
                return num if num > -999999 else None
            except:
                return None
        
        bachelor = safe_int(values[3]) or 0
        master = safe_int(values[4]) or 0
        professional = safe_int(values[5]) or 0
        doctorate = safe_int(values[6]) or 0
        total_edu = safe_int(values[7]) or 1
        
        college_educated = bachelor + master + professional + doctorate
        college_pct = (college_educated / total_edu * 100) if total_edu > 0 else 0
        
        unemployed = safe_int(values[8]) or 0
        labor_force = safe_int(values[9]) or 1
        unemployment_rate = (unemployed / labor_force * 100) if labor_force > 0 else 0
        
        return {
            "zip_code": zip_code,
            "state": state_code,
            "name": values[0],
            "median_income": safe_int(values[1]),
            "population": safe_int(values[2]),
            "college_educated_pct": round(college_pct, 1),
            "unemployment_rate": round(unemployment_rate, 1),
            "median_age": safe_int(values[10]),
            "data_source": "US Census Bureau ACS 2021"
        }
    except Exception as e:
        print(f"Error fetching ZIP {zip_code}: {str(e)}")
        return None


def lambda_handler(event, context):
    """Main Lambda function - fetches Census data for all ZIP codes"""
    
    print("=" * 70)
    print("Census API Fetcher Lambda - Starting...")
    print("=" * 70)
    
    # Define all target ZIP codes by state
    zip_codes = {
        'MA': ['02101', '02108', '02109', '02110', '02111', '02115', '02116', 
               '02118', '02119', '02120', '02138', '02139', '02140', '02141', 
               '02142', '02143', '02144', '02145', '02446', '02447', '02130', 
               '02131', '02132'],
        'CA': ['90001', '90002', '90003', '90004', '90005', '90012', '90013', 
               '90014', '90015', '90016', '90024', '90025', '90026', '90027', 
               '90028', '94102', '94103', '94104', '94105', '94107', '94108', 
               '94109', '94110', '94111', '94112', '92101', '92102', '92103', 
               '92104', '92105', '92106', '92107', '92108', '92109', '92110'],
        'NY': ['10001', '10002', '10003', '10004', '10005', '10009', '10010', 
               '10011', '10012', '10013', '10016', '10017', '10018', '10019', 
               '10020', '11201', '11205', '11206', '11211', '11215', '11217', 
               '11221', '11222', '11225', '11226', '11101', '11102', '11103', 
               '11104', '11105']
    }
    
    census_data = []
    total_zips = sum(len(zips) for zips in zip_codes.values())
    current = 0
    
    print(f"\nFetching data for {total_zips} ZIP codes...")
    
    for state, zips in zip_codes.items():
        print(f"Processing {state} ({len(zips)} ZIP codes)...")
        state_success = 0
        
        for zip_code in zips:
            current += 1
            print(f"  [{current}/{total_zips}] Fetching ZIP {zip_code}...", end=" ")
            
            data = fetch_census_data_for_zip(zip_code, state)
            
            if data:
                census_data.append(data)
                state_success += 1
                income = data.get('median_income')
                if income:
                    print(f"Income: ${income:,}")
                else:
                    print("(No income data)")
            else:
                print("Failed")
            
            time.sleep(0.1)
        
        print(f"{state}: {state_success}/{len(zips)} successful")
    
    # Save to S3
    if census_data:
        print(f"\n💾 Saving {len(census_data)} records to S3...")
        
        json_content = json.dumps(census_data, indent=2)
        
        s3_client.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=OUTPUT_KEY,
            Body=json_content.encode('utf-8'),
            ContentType='application/json'
        )
        
        # Calculate statistics
        by_state = {}
        total_income = 0
        income_count = 0
        
        for record in census_data:
            state = record['state']
            by_state[state] = by_state.get(state, 0) + 1
            if record.get('median_income'):
                total_income += record['median_income']
                income_count += 1
        
        avg_income = total_income / income_count if income_count > 0 else 0
        
        print("\n" + "=" * 70)
        print("SUCCESS!")
        print(f"Total ZIP codes fetched: {len(census_data)}")
        print(f"By state: {by_state}")
        print(f"Average median income: ${avg_income:,.0f}")
        print(f"Saved to: s3://{OUTPUT_BUCKET}/{OUTPUT_KEY}")
        print("=" * 70)
        
        return {
            'statusCode': 200,
            'message': 'Census data fetched and saved successfully!',
            'total_zip_codes': len(census_data),
            'by_state': by_state,
            'average_median_income': round(avg_income, 2),
            's3_location': f's3://{OUTPUT_BUCKET}/{OUTPUT_KEY}'
        }
    
    else:
        return {
            'statusCode': 500,
            'message': 'No census data retrieved'
        }