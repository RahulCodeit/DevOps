# lambda_function.py (Version 3 - with Totals)
import boto3
import csv
import io
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
​
# Account Name Mapping
# Please ensure this is filled in with your actual account names.
ACCOUNT_NAMES = {
    "446666283313": "Your Account Name",
    "Your Account ID": "Your Account Name",
...
}
​
# Initialize clients and variables from environment
sts_client = boto3.client('sts')
slack_token = os.environ.get('SLACK_BOT_TOKEN')
slack_channel_id = os.environ.get('SLACK_CHANNEL_ID')
member_account_role_name = os.environ.get('MEMBER_ACCOUNT_ROLE_NAME')
member_accounts_str = os.environ.get('MEMBER_ACCOUNTS')
slack_client = WebClient(token=slack_token)
​
def get_previous_month_dates():
    today = datetime.now()
    first_day_current_month = today.replace(day=1)
    last_day_previous_month = first_day_current_month - relativedelta(days=1)
    first_day_previous_month = last_day_previous_month.replace(day=1)
    return first_day_previous_month.strftime('%Y-%m-%d'), last_day_previous_month.strftime('%Y-%m-%d')
​
def lambda_handler(event, context):
    print("Starting AWS monthly cost report generation...")
​
    if not all([slack_token, slack_channel_id, member_account_role_name, member_accounts_str]):
        print("Error: Environment variables are not fully configured.")
        return {"statusCode": 500, "body": "Missing environment variables"}
​
    member_accounts = [acc_id.strip() for acc_id in member_accounts_str.split(',')]
    
    start_date, end_date = get_previous_month_dates()
    report_month_year = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m')
    print(f"Report period: {start_date} to {end_date}")
​
    csv_output = io.StringIO()
    csv_writer = csv.writer(csv_output)
    csv_writer.writerow(['Account ID', 'Account Name', 'Month', 'NetAmortizedCost', 'UnblendedCost'])
    
    # --- NEW --- Initialize variables to store totals
    total_net_amortized = 0.0
    total_unblended = 0.0
​
    for account_id in member_accounts:
        print(f"Processing account: {account_id}")
        
        account_name = ACCOUNT_NAMES.get(account_id, "Name Not Found")
        
        try:
            role_arn = f"arn:aws:iam::{account_id}:role/{member_account_role_name}"
            assumed_role = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName="CrossAccountCostExplorerSession"
            )
            
            credentials = assumed_role['Credentials']
            cost_explorer = boto3.client('ce',
                                         aws_access_key_id=credentials['AccessKeyId'],
                                         aws_secret_access_key=credentials['SecretAccessKey'],
                                         aws_session_token=credentials['SessionToken'])
            
            response = cost_explorer.get_cost_and_usage(
                TimePeriod={'Start': start_date, 'End': end_date},
                Granularity='MONTHLY',
                Metrics=['NetAmortizedCost', 'UnblendedCost'],
                GroupBy=[{'Type': 'DIMENSION', 'Key': 'LINKED_ACCOUNT'}]
            )
            
            cost_found = False
            if response.get('ResultsByTime'):
                for group in response['ResultsByTime'][0].get('Groups', []):
                    if group['Keys'][0] == account_id:
                        metrics = group['Metrics']
                        net_amortized_str = metrics.get('NetAmortizedCost', {}).get('Amount', '0.0')
                        unblended_str = metrics.get('UnblendedCost', {}).get('Amount', '0.0')
                        
                        try:
                            # --- MODIFIED --- Add to totals when processing valid costs
                            total_net_amortized += float(net_amortized_str)
                            total_unblended += float(unblended_str)
​
                            net_amortized_formatted = f"{float(net_amortized_str):.2f}"
                            unblended_formatted = f"{float(unblended_str):.2f}"
                        except ValueError:
                            net_amortized_formatted = net_amortized_str
                            unblended_formatted = unblended_str
​
                        csv_writer.writerow([account_id, account_name, report_month_year, net_amortized_formatted, unblended_formatted])
                        print(f"  Data for {account_id} ({account_name}): NetAmortized={net_amortized_formatted}, Unblended={unblended_formatted}")
                        cost_found = True
                        break
            
            if not cost_found:
                print(f"  No specific cost data returned for account {account_id}. Adding row with zero costs.")
                csv_writer.writerow([account_id, account_name, report_month_year, "0.00", "0.00"])
​
        except Exception as e:
            print(f"  ERROR processing account {account_id}: {str(e)}")
            csv_writer.writerow([account_id, account_name, report_month_year, "ERROR", "ERROR"])
​
    # --- NEW --- Add the final total row to the CSV after the loop
    print("All accounts processed. Calculating and adding total row.")
    total_net_amortized_formatted = f"{total_net_amortized:.2f}"
    total_unblended_formatted = f"{total_unblended:.2f}"
    
    # Add a blank row for spacing
    csv_writer.writerow([]) 
    
    # Write the total row, aligning columns correctly
    csv_writer.writerow(['Total', '', '', total_net_amortized_formatted, total_unblended_formatted])
​
    csv_file_content = csv_output.getvalue()
    csv_filename = f"aws_cost_{report_month_year}.csv"
    slack_message_text = "AWS monthly cost"
​
    try:
        print(f"Attempting to upload {csv_filename} to Slack channel {slack_channel_id}...")
        slack_client.files_upload_v2(
            channel=slack_channel_id,
            content=csv_file_content,
            filename=csv_filename,
            initial_comment=slack_message_text
        )
        print("File uploaded successfully to Slack.")
    except SlackApiError as e:
        error_message = f"Error uploading file to Slack: {e.response['error']}"
        print(error_message)
        return {"statusCode": 500, "body": error_message}
​
    return {"statusCode": 200, "body": "Report generated and sent to Slack successfully"}
