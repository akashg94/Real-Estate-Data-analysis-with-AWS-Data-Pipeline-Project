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
        'MA': ['01002', '01013', '01020', '01038', '01056', '01060', '01082', '01085', '01089', '01108',
               '01119', '01201', '01253', '01430', '01431', '01434', '01462', '01521', '01534', '01550',
               '01566', '01581', '01588', '01603', '01604', '01605', '01606', '01609', '01612', '01720',
               '01730', '01770', '01801', '01803', '01826', '01830', '01832', '01867', '01887', '01902',
               '01960', '01969', '01983', '01985', '02025', '02052', '02054', '02062', '02072', '02108',
               '02111', '02113', '02115', '02116', '02118', '02124', '02125', '02127', '02128', '02129',
               '02130', '02132', '02136', '02143', '02148', '02149', '02151', '02186', '02188', '02322',
               '02339', '02341', '02452', '02461', '02472', '02481', '02493', '02536', '02537', '02556',
               '02648', '02650', '02664', '02703', '02739'],
        'CA': ['90035', '90043', '90049', '90069', '90211', '90220', '90247', '90265', '90293', '90301',
               '90405', '90501', '90631', '90802', '90805', '90813', '91105', '91214', '91304', '91325',
               '91350', '91387', '91390', '91605', '91702', '91710', '91761', '91786', '91801', '91945',
               '92019', '92054', '92057', '92069', '92101', '92103', '92111', '92253', '92307', '92313',
               '92316', '92336', '92342', '92359', '92363', '92392', '92503', '92544', '92563', '92592',
               '92629', '92679', '92870', '93003', '93230', '93286', '93312', '93446', '93501', '93551',
               '93619', '94025', '94110', '94112', '94123', '94536', '94538', '94565', '94606', '94607',
               '94611', '94803', '94930', '94947', '94960', '95018', '95051', '95111', '95135', '95205',
               '95252', '95337', '95350', '95409', '95423', '95626', '95653', '95662', '95747', '95820',
               '95827', '95829', '95945', '95971', '95991', '96001', '96057', '96106', '96150'],
        'NY': ['10011', '10014', '10017', '10024', '10027', '10028', '10029', '10128', '10301', '10303',
               '10457', '10466', '10468', '10509', '10710', '11005', '11201', '11204', '11213', '11216',
               '11217', '11218', '11219', '11221', '11229', '11232', '11363', '11375', '11377', '11385',
               '11413', '11419', '11422', '11510', '11590', '11713', '11720', '11753', '11764', '11768',
               '11901', '11930', '11937', '12009', '12053', '12139', '12144', '12177', '12180', '12198',
               '12209', '12401', '12423', '12524', '12542', '12553', '12563', '12571', '12811', '12865',
               '12887', '12901', '12928', '12986', '13021', '13057', '13066', '13080', '13148', '13205',
               '13209', '13601', '13619', '13662', '13736', '13750', '13780', '14028', '14132', '14141',
               '14201', '14217', '14425', '14450', '14471', '14609', '14615', '14753', '14769', '14870',
               '14882'],
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
