import clickhouse_connect
from clickhouse_connect.driver import httputil
from datetime import datetime, timedelta
import pandas as pd
import calendar
import smtplib
from email.message import EmailMessage
import os

# Add constant for aggregator order at the top level
AGGREGATOR_ORDER = [
    'Karix Mobile Private Limited',
    'Valuefirst Digital Media Private Limited',
    'ICS MOBILE PVT LTD',
    'TANLA PLATFORMS LTD'
]

def get_clickhouse_client():
    insight_host = '10.10.9.31'
    username = 'dashboard_user'
    password = 'dA5Hb0#6duS36'
    port = 8123
    
    big_pool_mgr = httputil.get_pool_manager(maxsize=30, num_pools=20)
    
    return clickhouse_connect.get_client(
        host=insight_host,
        username=username,
        password=password,
        port=port,
        query_limit=10000000000,
        connect_timeout='100000',
        send_receive_timeout='300000',
        settings={
            'max_insert_threads': 32,
            'max_query_size': 1000000000,
            'receive_timeout': 0,
            'max_memory_usage': 101737418240
        },
        pool_mgr=big_pool_mgr
    )

def get_date_ranges():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    # FTD (yesterday to yesterday)
    ftd_start = yesterday.strftime('%Y-%m-%d')
    ftd_end = yesterday.strftime('%Y-%m-%d')
    
    # MTD (start of current month to yesterday)
    mtd_start = today.replace(day=1).strftime('%Y-%m-%d')
    mtd_end = yesterday.strftime('%Y-%m-%d')
    
    # LMTD (start of last month to same day last month)
    first_of_month = today.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    lmtd_start = last_month.replace(day=1).strftime('%Y-%m-%d')
    lmtd_end = min(
        last_month.replace(day=yesterday.day),
        last_month.replace(day=calendar.monthrange(last_month.year, last_month.month)[1])
    ).strftime('%Y-%m-%d')
    
    return {
        'FTD': (ftd_start, ftd_end),
        'MTD': (mtd_start, mtd_end),
        'LMTD': (lmtd_start, lmtd_end)
    }

def run_traffic_summary_query(client, start_date, end_date):
    query = """
    SELECT * FROM stats.vw_traffic_summary 
    WHERE dtDate BETWEEN '{start_date}' AND '{end_date}'
    """
    
    result = client.query(query.format(
        start_date=start_date,
        end_date=end_date
    ))
    
    return pd.DataFrame(result.result_set, columns=result.column_names)

def run_od_traffic_query(client, start_date, end_date):
    query = """
    select dtDate as Date,
           vcORGName as Aggregator,
           vcBrandName as Brand,
           vcAgentID as AgentID,
           vcAgentName as Agent,
           vcTrafficType as TrafficType,
           vcContentType as ContentType,
           vcTemplateType as TemplateType,
           iTextParts as Parts,
           sum(iTotalSubmitSuccess) as Received,
           sum(iTotalSentSuccess) as Sent,
           sum(iTotalDelivered) as Delivered
    from stats.vw_mt_stats
    where not (iTotalSubmitSuccess=0 and iTotalSentSuccess=0 and iTotalDelivered=0 
              and iTotalRead=0 and iTotalFailed=0 and iTotalExpired=0)
    and dtDate between '{start_date}' AND '{end_date}'
    and vcAgentID in ('maap8s3t0rw9jyvj','maaprf4actpm13wy','r2kXQolZRjF8TODR','3Sgzj0xFRe7zmI7x','maaphzi23qaz2kdj','maap87clp086lu1e','maapznhtiq6pqon6','maapg32ibsbwdp9o','maapkhjaftch76yv','maapi89sm1dlxdj9','maapINTl5sky1pft','mappbop7ayupqjcn','mapp3gkp4eqf1snw','mapp92lyi13nvb47','maap3vlgsytmadyy','maap2kotupcvwabj','maapfrpj8593nxvq','maap545nyhizicfe','maapc38dkb1va0hc','maapwd8op2hxvwnb','maap70b7egkw5b9w','maapu3rxs3poemcm','maapb7weyp4gg6n2','maapc3kzcp8vqwnh','maapz8xg5eh798b5','maapms46kk6m1vrh','maap7qae70nguiqz','maapolrbhfeicbjz','maapu9dkzbv4w5s2','maapwd8op2hxvwnb','maapmlr6jrgtktnt','maapu7j1l5yil0j0','maapudpeijl77e3k','maap7ebx3pd2fi79','maapsmmvsi1bgnyy','maapche39cha40ha','maaposmo8ka7lq7k','maapjks6dv93898j','maap3vlgsytmadyy','mapp3gkp4eqf1snw','mapp92lyi13nvb47','maapfrpj8593nxvq','maap545nyhizicfe','maapg32ibsbwdp9o','maapznhtiq6pqon6','maapc38dkb1va0hc','1E6BQ7PvGqrYonm9','maapdx13uq3d8873','61QyuSw6rfyu90LF','9IiX9UtzGBlARVXO','qyI4xZkJo44DiTpR','jWWmMvVknKu67Nf8','cTiftYuyUt26Vafl','maapdtf6qxlcqitz','2BF7iGx19xmPuBbw','x6bWpW5hHcyFaTCY','maapms46kk6m1vrh','maap7qae70nguiqz','1dkAnRKJSSBgvDFQ','9qFGSfUhm1axbrQY','maapu9dkzbv4w5s2','maapolrbhfeicbjz','oZZiOFk247qRH4qx','ygasrFMsBO3Yi5Nc','maapdvj8a7n2drfl','maapvls4mjfzdzc3','','maap05clsdnyresz','maapz8xg5eh798b5','maapc3kzcp8vqwnh','F2Yzi41ow9wcCQ3Y','fI3KJZQW1CnnVupr','m0qah7pHf9cvYoot','ROPGUkswD5TL6qUM','QrXtm1f2AwOUhM9G','m7EnVZVWaDme8pbn','jzz61RUO7BDqanpW','maapolrbhfeicbjz','maapu9dkzbv4w5s2','N12zUFpEPVT8BOj3','8SFjofdfvxOSBCa6')
    group by 1,2,3,4,5,6,7,8,9
    order by 1"""
    
    result = client.query(query.format(
        start_date=start_date,
        end_date=end_date
    ))
    
    return pd.DataFrame(result.result_set, columns=result.column_names)

def load_mapping():
    """Load the agent type mapping from CSV"""
    mapping_df = pd.read_csv('mapping.csv')
    return dict(zip(mapping_df['vcAgentID'], mapping_df['vcType']))

def update_traffic_df_with_type(df, mapping):
    """Update dataframe with agent types from mapping"""
    # Create a new column for agent type
    df['vcType'] = df['vcAgentID'].map(mapping)  # Changed from 'AgentID' to 'vcAgentID'
    
    # Fill any null values with 'Enterprise'
    df['vcType'] = df['vcType'].fillna('Enterprise')
    
    return df


def create_traffic_summary_pivot(dfs):
    """Create overall traffic summary pivot table"""
    combined_df = pd.concat([
        dfs['traffic_summary_ftd'].assign(Period='FTD'),
        dfs['traffic_summary_mtd'].assign(Period='MTD'),
        dfs['traffic_summary_lmtd'].assign(Period='LMTD')
    ])
    
    # Create initial pivot table
    pivot = pd.pivot_table(
        combined_df,
        index=['vcORGName', 'vcType'],
        columns=['Period'],
        values=['iTotalSentSuccess'],
        aggfunc='sum'
    )
    
    # Flatten column names
    pivot.columns = [f"{col[1]}" for col in pivot.columns]
    
    # Create a new DataFrame with ordered data
    ordered_data = []
    type_totals = {'CPL': 0, 'Enterprise': 0, 'OD': 0}
    
    # Process each aggregator in order
    for agg in AGGREGATOR_ORDER:
        if agg in pivot.index.get_level_values('vcORGName'):
            # Add aggregator total first
            agg_total = pivot.loc[agg].sum()
            ordered_data.append((agg, 'Total', agg_total))
            
            # Add individual rows for this aggregator
            for idx in pivot.loc[agg].index:
                type_data = pivot.loc[(agg, idx)]
                ordered_data.append((agg, idx, type_data))
                # Add to type totals
                type_totals[idx] += type_data.sum()
    
    # Add type totals
    ordered_data.append(('Total CPL', 'Total', pd.Series([type_totals['CPL']] * len(pivot.columns), index=pivot.columns)))
    ordered_data.append(('Total Enterprise', 'Total', pd.Series([type_totals['Enterprise']] * len(pivot.columns), index=pivot.columns)))
    ordered_data.append(('Total OD', 'Total', pd.Series([type_totals['OD']] * len(pivot.columns), index=pivot.columns)))
    
    # Add grand total
    grand_total = sum(type_totals.values())
    ordered_data.append(('Grand Total', 'Total', pd.Series([grand_total] * len(pivot.columns), index=pivot.columns)))
    
    # Create new DataFrame from ordered data with multi-level index
    new_pivot = pd.DataFrame(
        [row[2] for row in ordered_data],
        index=pd.MultiIndex.from_tuples([(row[0], row[1]) for row in ordered_data], names=['Aggregator', 'Type'])
    )
    
    return new_pivot

def create_od_traffic_pivot(dfs):
    """Create OD traffic summary pivot table"""
    combined_df = pd.concat([
        dfs['od_traffic_ftd'].assign(Period='FTD'),
        dfs['od_traffic_mtd'].assign(Period='MTD'),
        dfs['od_traffic_lmtd'].assign(Period='LMTD')
    ])
    
    # Create pivot table with periods as main columns
    pivot = pd.pivot_table(
        combined_df,
        index=['Aggregator', 'ContentType'],
        columns=['Period'],
        values=['Sent', 'Delivered'],
        aggfunc='sum'
    )
    
    # Reorder the columns to have Period as main level
    periods = ['FTD', 'MTD', 'LMTD']
    metrics = ['Sent', 'Delivered']
    
    # Create new column order with Period as the main level
    new_cols = []
    for period in periods:
        for metric in metrics:
            new_cols.append((period, metric))
    
    # Create new column index
    new_col_index = pd.MultiIndex.from_tuples(new_cols, names=['Period', 'Metric'])
    
    # Reorder the columns
    reordered_pivot = pivot.reindex(columns=new_col_index)
    
    # Process aggregators in order
    ordered_data = []
    for agg in AGGREGATOR_ORDER:
        if agg in reordered_pivot.index.get_level_values('Aggregator'):
            # Add aggregator total first
            agg_total = reordered_pivot.loc[agg].sum()
            ordered_data.append((agg, 'Total', agg_total))
            
            # Add content type rows
            for content_type in reordered_pivot.loc[agg].index:
                ordered_data.append((agg, content_type, reordered_pivot.loc[(agg, content_type)]))
    
    # Create new DataFrame from ordered data
    new_pivot = pd.DataFrame(
        [row[2] for row in ordered_data],
        index=pd.MultiIndex.from_tuples([(row[0], row[1]) for row in ordered_data])
    )
    
    return new_pivot

def load_mapping():
    """Load the agent type mapping from CSV"""
    mapping_df = pd.read_csv('mapping.csv')
    return dict(zip(mapping_df['vcAgentID'], mapping_df['vcType']))

def update_traffic_df_with_type(df, mapping):
    """Update dataframe with agent types from mapping"""
    # Create a new column for agent type
    df['vcType'] = df['vcAgentID'].map(mapping)  # Changed from 'AgentID' to 'vcAgentID'
    
    # Fill any null values with 'Enterprise'
    df['vcType'] = df['vcType'].fillna('Enterprise')
    
    return df

def create_od_traffic_pivot(dfs):
    """Create OD traffic summary pivot table"""
    combined_df = pd.concat([
        dfs['od_traffic_ftd'].assign(Period='FTD'),
        dfs['od_traffic_mtd'].assign(Period='MTD'),
        dfs['od_traffic_lmtd'].assign(Period='LMTD')
    ])
    
    # Create pivot table with proper hierarchy
    pivot = pd.pivot_table(
        combined_df,
        index=['Aggregator', 'ContentType'],
        columns=['Period'],
        values=['Sent', 'Delivered'],
        aggfunc='sum'
    )
    
    # Reorder columns to show Sent/Delivered under each period
    column_order = []
    for period in ['FTD', 'MTD', 'LMTD']:
        column_order.extend([('Sent', period), ('Delivered', period)])
    pivot = pivot.reindex(columns=column_order)
    
    # Process aggregators in order
    ordered_data = []
    for agg in AGGREGATOR_ORDER:
        if agg in pivot.index.get_level_values('Aggregator'):
            # Add aggregator total first
            agg_total = pivot.loc[agg].sum()
            ordered_data.append((agg, 'Total', agg_total))
            
            # Add content type rows
            for content_type in pivot.loc[agg].index:
                ordered_data.append((agg, content_type, pivot.loc[(agg, content_type)]))
    
    # Create new DataFrame from ordered data
    new_pivot = pd.DataFrame([row[2] for row in ordered_data], 
                           index=pd.MultiIndex.from_tuples([(row[0], row[1]) for row in ordered_data]))
    
    return new_pivot

def create_od_summary_pivot(mtd_df):
    """Create OD Summary pivot table (3)"""
    # Filter the dataframe to only include Karix and Valuefirst
    aggregators = [
        'Karix Mobile Private Limited',
        'Valuefirst Digital Media Private Limited'
    ]
    filtered_df = mtd_df[mtd_df['Aggregator'].isin(aggregators)]
    
    # Create pivot table with Aggregators as main columns and metrics as sub-columns
    pivot = pd.pivot_table(
        filtered_df,
        index=['Agent'],
        columns=['Aggregator'],
        values=['Sent', 'Delivered'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Reorder columns to show Sent/Delivered under each aggregator
    new_cols = []
    metrics = ['Sent', 'Delivered']
    
    for agg in aggregators:
        for metric in metrics:
            new_cols.append((agg, metric))
    
    # Create new column index and reorder
    new_col_index = pd.MultiIndex.from_tuples(new_cols, names=['Aggregator', 'Metric'])
    pivot = pivot.reindex(columns=new_col_index)
    
    # Add total columns
    for metric in metrics:
        pivot[('Total', metric)] = pivot.xs(metric, axis=1, level=1).sum(axis=1)
    
    print("Created OD Summary pivot table")
    return pivot

def create_volume_pivot(mtd_df):
    """Create Volume Summary pivot table (4)"""
    df = mtd_df.copy()
    
    # Create RCS and SMS volumes
    df['SMS_Vol'] = df['Delivered'] * df['Parts']
    df['RCS_Vol'] = df['Delivered']
    
    # Create custom index mapping
    def get_category(row):
        if row['ContentType'] in ['Basic', 'Single']:
            return f"{row['ContentType']}"
        return None
    
    df['Category'] = df.apply(get_category, axis=1)
    
    # Create separate pivots for RCS and SMS
    rcs_pivot = pd.pivot_table(
        df,
        index=['Category'],
        columns=['Date'],
        values=['RCS_Vol'],
        aggfunc='sum'
    ).fillna(0)
    
    sms_pivot = pd.pivot_table(
        df,
        index=['Category'],
        columns=['Date'],
        values=['SMS_Vol'],
        aggfunc='sum'
    ).fillna(0)
    
    # Calculate totals
    rcs_total = pd.DataFrame(rcs_pivot.sum(), columns=['RCS Total']).T
    sms_total = pd.DataFrame(sms_pivot.sum(), columns=['SMS Total']).T
    
    # Combine all parts with the desired order
    final_pivot = pd.concat([
        rcs_total,
        rcs_pivot.loc[['Basic', 'Single']],
        sms_total,
        sms_pivot.loc[['Basic', 'Single']]
    ])
    
    # Clean up the index
    final_pivot.index = ['RCS Total', 'Basic', 'Single', 'SMS Total', 'Basic', 'Single']
    
    print("Created Volume Summary pivot table")
    return final_pivot

def create_summary_type_pivot(mtd_df):
    """Create Summary by Type pivot table (5)"""
    # First create the pivot table
    pivot = pd.pivot_table(
        mtd_df,
        index=['dtDate'],
        columns=['vcType'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum'
    )
    
    # Define the desired column order
    types = ['CPL', 'OD', 'Enterprise']
    metrics = ['iTotalSentSuccess', 'iTotalDelivered']
    
    # Create new column order
    new_cols = []
    for type_ in types:
        for metric in metrics:
            new_cols.append((type_, metric))
    
    # Add total columns
    total_sent = pivot['iTotalSentSuccess'].sum(axis=1)
    total_delivered = pivot['iTotalDelivered'].sum(axis=1)
    
    # Reorder columns and rename
    pivot = pivot.reindex(columns=pd.MultiIndex.from_tuples(new_cols))
    
    # Add totals
    pivot[('Total', 'iTotalSentSuccess')] = total_sent
    pivot[('Total', 'iTotalDelivered')] = total_delivered
    
    # Rename columns for better readability
    pivot = pivot.rename(columns={
        'iTotalSentSuccess': 'Sent',
        'iTotalDelivered': 'Delivered'
    })
    
    print("Created Summary by Type pivot table")
    return pivot

def create_daywise_od_summary(mtd_df):
    """Create Daywise OD Summary pivot table (6)"""
    # Create pivot table with Date as the main column level
    pivot = pd.pivot_table(
        mtd_df,
        index=['Agent', 'ContentType'],
        columns=['Date'],
        values=['Sent', 'Delivered'],
        aggfunc='sum'
    )
    
    # Reorder column levels to have Date as the main level
    dates = pivot.columns.get_level_values(1).unique()
    metrics = ['Sent', 'Delivered']
    
    # Create new column order
    new_cols = []
    for date in dates:
        for metric in metrics:
            new_cols.append((date, metric))
    
    # Create new column index and reorder
    new_col_index = pd.MultiIndex.from_tuples(new_cols, names=['Date', 'Metric'])
    pivot = pivot.reindex(columns=new_col_index)
    
    # Add total columns
    totals = {}
    for metric in metrics:
        totals[metric] = pivot.xs(metric, axis=1, level=1).sum(axis=1)
    
    for metric in metrics:
        pivot[('Total', metric)] = totals[metric]
    
    print("Created Daywise OD Summary pivot table")
    return pivot

def create_daywise_traffic_pivot(mtd_df):
    """Create Daywise Traffic Summary pivot table (7)"""
    # Convert date column to date type if it's not already
    mtd_df['dtDate'] = pd.to_datetime(mtd_df['dtDate']).dt.date
    
    # Create base pivot
    pivot = pd.pivot_table(
        mtd_df,
        index=['dtDate', 'vcORGName', 'vcAgentName'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum'
    )
    
    # Create a list to store all parts of the final pivot
    pivot_parts = [pivot]
    
    # Calculate and add aggregator subtotals for each date
    for date in pivot.index.get_level_values('dtDate').unique():
        date_data = pivot.xs(date, level='dtDate')
        agg_subtotals = date_data.groupby('vcORGName').sum()
        
        # Add the 'Total' level for vcAgentName
        agg_subtotals['vcAgentName'] = 'Total'
        agg_subtotals = agg_subtotals.set_index('vcAgentName', append=True)
        
        # Add the date level back
        agg_subtotals = pd.concat({date: agg_subtotals}, names=['dtDate'])
        pivot_parts.append(agg_subtotals)
    
    # Calculate and add date totals
    date_totals = pivot.groupby('dtDate').sum()
    date_totals = pd.concat({
        ('Total', 'Total'): date_totals
    }, names=['vcORGName', 'vcAgentName']).swaplevel(0,1).swaplevel(1,2)
    pivot_parts.append(date_totals)
    
    # Combine all parts
    final_pivot = pd.concat(pivot_parts)
    
    # Sort the index to keep everything organized
    final_pivot = final_pivot.sort_index()
    
    print("Created Daywise Traffic Summary pivot table")
    return final_pivot

def create_aggregator_pivot(mtd_df):
    """Create Aggregator Summary pivot table (8)"""
    pivot = pd.pivot_table(
        mtd_df,
        index=['vcORGName', 'vcAgentName'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum'
    )
    
    print("Created Aggregator Summary pivot table")
    return pivot

def create_botwise_pivot(mtd_df):
    """Create Botwise Summary pivot table (9)"""
    pivot = pd.pivot_table(
        mtd_df,
        index=['vcAgentName', 'vcORGName'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum'
    )
    
    print("Created Botwise Summary pivot table")
    return pivot

def export_to_excel(od_summary_pivot, volume_pivot, summary_type_pivot, 
                   daywise_od_pivot, daywise_traffic_pivot,
                   aggregator_pivot, botwise_pivot, filename="traffic_report.xlsx"):
    """Export pivot tables to Excel file (excluding the email summary tables)"""
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        # Write only the detailed pivot tables to separate sheets
        pivots = {
            "OD_Summary": od_summary_pivot,
            "Volume_Summary": volume_pivot,
            "Type_Summary": summary_type_pivot,
            "Daywise_OD_Summary": daywise_od_pivot,
            "Daywise_Traffic": daywise_traffic_pivot,
            "Aggregator_Summary": aggregator_pivot,
            "Botwise_Summary": botwise_pivot
        }
        
        for sheet_name, pivot in pivots.items():
            pivot.to_excel(writer, sheet_name=sheet_name)
            
            # Auto-adjust columns width
            worksheet = writer.sheets[sheet_name]
            
            # Get unique column letters and their maximum widths
            col_widths = {}
            for column in worksheet.columns:
                # Get the first cell that has a column_letter attribute
                col_letter = None
                max_length = 0
                
                for cell in column:
                    try:
                        # Try to get column letter from the cell
                        if not col_letter and hasattr(cell, 'column_letter'):
                            col_letter = cell.column_letter
                        
                        # Update max length if cell has a value
                        if cell.value:
                            max_length = max(max_length, len(str(cell.value)))
                    except:
                        continue
                
                # Only set width if we found a valid column letter
                if col_letter:
                    col_widths[col_letter] = max(col_widths.get(col_letter, 0), max_length + 2)
            
            # Apply the calculated widths
            for col_letter, width in col_widths.items():
                worksheet.column_dimensions[col_letter].width = width
    
    print(f"\nDetailed pivot tables have been exported to {filename}")

def create_clean_table(df):
    """Convert DataFrame to HTML table with styling"""
    # Convert DataFrame to HTML with styling
    html = df.to_html(
        classes='clean-table',
        float_format=lambda x: '{:.2f}'.format(x),
        border=1,
        justify='left'
    )
    
    # Add clean, minimal CSS with improved styling
    styled_html = f"""
    <style>
        .clean-table {{
            border-collapse: collapse;
            font-family: 'Calibri', sans-serif;
            font-size: 11pt;
            width: 100%;
            margin: 10px 0;
        }}
        .clean-table th, .clean-table td {{
            padding: 4px 8px;
            border: 1px solid #dddddd;
            text-align: left;
        }}
        .clean-table th {{
            background-color: #f8f9fa;
            color: #333333;
            font-weight: normal;
        }}
        .clean-table tr.bold td {{
            font-weight: bold;
            background-color: #f8f9fa;
        }}
        .clean-table tr.total td {{
            font-weight: bold;
            background-color: #e9ecef;
        }}
        .clean-table tr.grand-total td {{
            font-weight: bold;
            background-color: #dee2e6;
            color: #28a745;
        }}
    </style>
    """
    
    # List of patterns for aggregator rows and total rows
    aggregators = [
        'Karix Mobile Private Limited',
        'Valuefirst Digital Media Private Limited',
        'ICS MOBILE PVT LTD',
        'TANLA PLATFORMS LTD'
    ]
    
    total_patterns = [
        'Total CPL',
        'Total Enterprise',
        'Total OD'
    ]
    
    # Add row classes for styling
    html_lines = html.split('\n')
    for i, line in enumerate(html_lines):
        if '<tr>' in line and '<td' in line:
            if 'Grand Total' in line:
                html_lines[i] = line.replace('<tr>', '<tr class="grand-total">')
            elif any(total in line for total in total_patterns):
                html_lines[i] = line.replace('<tr>', '<tr class="total">')
            elif any(agg in line for agg in aggregators):
                html_lines[i] = line.replace('<tr>', '<tr class="bold">')
    
    styled_html += '\n'.join(html_lines)
    return styled_html

def send_summary_email(traffic_pivot, od_pivot, excel_path, recipients):
    """Send email with pivot tables"""
    try:
        email = EmailMessage()
        email['Subject'] = f"RCS Traffic Summary Report | {pd.Timestamp.now().strftime('%Y-%m-%d')}"
        email['From'] = 'donotreply<donotreply@tanla.com>'
        email['To'] = ', '.join(recipients)
        
        body = f"""
        <html>
        <body style="font-family: 'Calibri', sans-serif; font-size: 11pt; color: #333333; line-height: 1.4;">
            <p>Dear Sir,</p>
            
            <p>Please find the RCS vol for {pd.Timestamp.now().strftime('%b\'%y')}.</p>
            
            <div style="margin: 20px 0;">
                <p style="font-weight: bold; margin-bottom: 8px;">Overall Traffic Summary:</p>
                <p style="color: #666666; font-size: 10pt; margin-bottom: 8px;">*vol in Millions</p>
                {create_clean_table(traffic_pivot)}
            </div>
            
            <div style="margin: 20px 0;">
                <p style="font-weight: bold; margin-bottom: 8px;">OD Traffic Summary:</p>
                <p style="color: #666666; font-size: 10pt; margin-bottom: 8px;">*vol in Millions</p>
                {create_clean_table(od_pivot)}
            </div>
            
            <p style="color: #666666; margin-top: 20px;">
                Please find the detailed analysis in the attached Excel file.
            </p>
        </body>
        </html>
        """
        
        email.set_content(body, subtype='html')
        
        # Attach Excel file
        if os.path.exists(excel_path):
            with open(excel_path, 'rb') as f:
                email.add_attachment(
                    f.read(),
                    maintype='application',
                    subtype='xlsx',
                    filename='RCS_Analysis.xlsx'
                )
        
        # Send email using SMTP
        with smtplib.SMTP('smtp.office365.com', 25) as server:
            server.connect('smtp.office365.com', 25)
            server.ehlo()
            server.starttls()
            server.login('donotreply-ildhub@tanla.com', 'Jar45492')
            server.send_message(email)
            
        print("Email sent successfully!")
        return True
        
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        return False

def main():
    client = get_clickhouse_client()
    date_ranges = get_date_ranges()
    
    # Load the agent type mapping
    agent_mapping = load_mapping()
    
    # Dictionary to store all dataframes
    dfs = {}
    
    # Run both queries for each time period
    for period, (start_date, end_date) in date_ranges.items():
        print(f"\nFetching data for {period} ({start_date} to {end_date})")
        
        # Traffic Summary Query - Update with agent types
        traffic_summary_df = run_traffic_summary_query(client, start_date, end_date)
        dfs[f'traffic_summary_{period.lower()}'] = update_traffic_df_with_type(
            traffic_summary_df, agent_mapping
        )
        
        # OD Traffic Query - Leave as is
        dfs[f'od_traffic_{period.lower()}'] = run_od_traffic_query(
            client, start_date, end_date
        )
    
    # Create pivot tables
    traffic_pivot = create_traffic_summary_pivot(dfs)
    od_pivot = create_od_traffic_pivot(dfs)
    
    # Create additional pivot tables
    od_summary_pivot = create_od_summary_pivot(dfs['od_traffic_mtd'])
    volume_pivot = create_volume_pivot(dfs['od_traffic_mtd'])
    summary_type_pivot = create_summary_type_pivot(dfs['traffic_summary_mtd'])
    daywise_od_pivot = create_daywise_od_summary(dfs['od_traffic_mtd'])
    daywise_traffic_pivot = create_daywise_traffic_pivot(dfs['traffic_summary_mtd'])
    aggregator_pivot = create_aggregator_pivot(dfs['traffic_summary_mtd'])
    botwise_pivot = create_botwise_pivot(dfs['traffic_summary_mtd'])
    
    # Export to Excel (excluding the email summary tables)
    excel_filename = "traffic_report.xlsx"
    export_to_excel(
        od_summary_pivot, volume_pivot,
        summary_type_pivot, daywise_od_pivot, daywise_traffic_pivot,
        aggregator_pivot, botwise_pivot, filename=excel_filename
    )
    
    print("\nPivot tables have been exported to", excel_filename)
    
    # Send email with summary
    recipients = ['nithin.didigam@tanla.com']  # Add more recipients as needed
    if send_summary_email(traffic_pivot, od_pivot, excel_filename, recipients):
        print("Email sent successfully with pivot tables and Excel attachment")
    else:
        print("Failed to send email")

if __name__ == "__main__":
    main()
