import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
import calendar

def get_clickhouse_client():
    insight_host = '10.10.9.31'
    username = 'dashboard_user'
    password = 'dA5Hb0#6duS36'
    port = 8123
    
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
        }
    )

def get_date_ranges():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    ftd_start = ftd_end = yesterday.strftime('%Y-%m-%d')
    mtd_start = today.replace(day=1).strftime('%Y-%m-%d')
    mtd_end = ftd_end
    
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

def fetch_data(query, start_date, end_date):
    client = get_clickhouse_client()
    query = query.format(start_date=start_date, end_date=end_date)
    result = client.query_df(query)
    return result

query1 = """
    SELECT * FROM stats.vw_traffic_summary 
    WHERE dtDate BETWEEN '{start_date}' AND '{end_date}'
    """

query2 = """
    SELECT dtDate as Date, vcORGName as Aggregator, vcBrandName as Brand, vcAgentID as AgentID,
           vcAgentName as Agent, vcTrafficType as TrafficType, vcContentType as ContentType,
           vcTemplateType as TemplateType, iTextParts as Parts,
           sum(iTotalSubmitSuccess) as Received, sum(iTotalSentSuccess) as Sent,
           sum(iTotalDelivered) as Delivered
    FROM stats.vw_mt_stats
    WHERE NOT (iTotalSubmitSuccess=0 AND iTotalSentSuccess=0 AND iTotalDelivered=0 
              AND iTotalRead=0 AND iTotalFailed=0 AND iTotalExpired=0)
    AND dtDate BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY 1
    """

date_ranges = get_date_ranges()

dfs = {}
for label, (start_date, end_date) in date_ranges.items():
    dfs[f"{label}_traffic"] = fetch_data(query1, start_date, end_date)
    dfs[f"{label}_OD"] = fetch_data(query2, start_date, end_date)

def update_df_with_type(df, mappings):
    """Update dataframe with agent types from mapping"""
    # Create a dictionary from mapping.csv
    type_dict = dict(zip(mappings['vcAgentID'], mappings['vcType']))
    
    # Add vcType column directly using map
    df['vcType'] = df['vcAgentID'].map(type_dict).fillna('Enterprise')
    
    return df

def add_volume_columns(df):
    """Add RCSVol and SMSVol columns to OD dataframes"""
    df['RCSVol'] = df['Delivered']
    df['SMSVol'] = df['Delivered'] * df['Parts']
    return df

def create_comparative_pivot(dfs):
    """Create pivot table comparing different date ranges"""
    pivot_data = []
    
    for period in ['FTD', 'MTD', 'LMTD']:
        df = dfs[f"{period}_traffic"]
        df['Period'] = period
        pivot_data.append(df)
    
    combined_df = pd.concat(pivot_data, ignore_index=True)
    
    # Create pivot table with vcType as secondary index
    pivot_table = combined_df.pivot_table(
        index=['vcORGName', 'vcType'],
        columns=['Period'],
        values=['iTotalSentSuccess'],
        aggfunc='sum',
        fill_value=0,
        margins=False
    ).round(0)  # Round to whole numbers
    
    # Flatten column names
    pivot_table.columns = [f'iTotalSentSuccess_{col[1]}' for col in pivot_table.columns]
    
    # Sort columns in specific order
    column_order = ['iTotalSentSuccess_FTD', 'iTotalSentSuccess_LMTD', 'iTotalSentSuccess_MTD']
    pivot_table = pivot_table.reindex(columns=column_order)
    
    return pivot_table

def create_od_pivot(dfs):
    """Create pivot table for OD data comparing different date ranges"""
    pivot_data = []
    
    for period in ['FTD', 'MTD', 'LMTD']:
        df = dfs[f"{period}_OD"]
        df['Period'] = period
        pivot_data.append(df)
    
    combined_df = pd.concat(pivot_data, ignore_index=True)
    
    # Create pivot table with date ranges as main columns and metrics as subcolumns
    pivot_table = combined_df.pivot_table(
        index=['Aggregator', 'ContentType'],
        columns='Period',
        values=['Sent', 'Delivered'],
        aggfunc='sum',
        fill_value=0,
        margins=False
    ).round(0)
    
    # Reorder columns to show FTD, LMTD, MTD
    column_order = ['FTD', 'LMTD', 'MTD']
    # Create proper MultiIndex for columns
    new_order = pd.MultiIndex.from_product(
        [['Sent', 'Delivered'], column_order],
        names=[None, 'Period']
    )
    pivot_table = pivot_table.reindex(columns=new_order)
    
    return pivot_table

def create_daily_volume_pivot(dfs):
    """Create pivot table for daily RCS and SMS volumes"""
    # Get MTD dataframe
    mtd_df = dfs['MTD_OD'].copy()
    
    # Create volume metrics
    mtd_df['RCS Volume'] = mtd_df['RCSVol']
    mtd_df['SMS Volume'] = mtd_df['SMSVol']
    
    # Melt the volume columns to create a single metric column
    volume_df = pd.melt(
        mtd_df,
        id_vars=['Date', 'ContentType'],
        value_vars=['RCS Volume', 'SMS Volume'],
        var_name='Volume Type',
        value_name='Volume'
    )
    
    # Create pivot table with dates as columns
    pivot_table = volume_df.pivot_table(
        index=['Volume Type', 'ContentType'],
        columns=['Date'],
        values='Volume',
        aggfunc='sum',
        fill_value=0
    ).round(0)
    
    # Sort columns by date
    pivot_table = pivot_table.reindex(columns=sorted(pivot_table.columns))
    
    return pivot_table

def create_od_summary_pivot(dfs):
    """Create OD summary pivot (Pivot4)"""
    mtd_df = dfs['MTD_OD']
    pivot_table = mtd_df.pivot_table(
        index=['Agent'],
        columns=['Aggregator'],
        values=['Sent', 'Delivered'],
        aggfunc='sum',
        fill_value=0
    ).round(0)
    return pivot_table

def create_traffic_by_type_pivot(dfs):
    """Create traffic by type pivot (Pivot5)"""
    mtd_df = dfs['MTD_traffic']
    pivot_table = mtd_df.pivot_table(
        index=['dtDate'],
        columns=['vcType'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum',
        fill_value=0
    ).round(0)
    return pivot_table

def create_daily_od_detail_pivot(dfs):
    """Create daily OD detail pivot (Pivot6)"""
    mtd_df = dfs['MTD_OD']
    pivot_table = mtd_df.pivot_table(
        index=['Aggregator', 'Agent', 'ContentType'],
        columns=['Date'],
        values=['Sent', 'Delivered'],
        aggfunc='sum',
        fill_value=0
    ).round(0)
    return pivot_table

def create_daily_traffic_detail_pivot(dfs):
    """Create daily traffic detail pivot (Pivot7)"""
    mtd_df = dfs['MTD_traffic']
    pivot_table = mtd_df.pivot_table(
        index=['dtDate', 'vcORGName', 'vcType', 'vcAgentName'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum',
        fill_value=0,
        margins=False
    ).round(0)
    
    # Optionally sort the index
    pivot_table = pivot_table.sort_index()
    
    return pivot_table

def create_traffic_by_agent_pivot(dfs):
    """Create traffic by agent pivot (Pivot8)"""
    mtd_df = dfs['MTD_traffic']
    pivot_table = mtd_df.pivot_table(
        index=['vcORGName', 'vcAgentName'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum',
        fill_value=0
    ).round(0)
    return pivot_table

def create_agent_aggregator_pivot(dfs):
    """Create agent-aggregator pivot (Pivot9)"""
    mtd_df = dfs['MTD_traffic']
    pivot_table = mtd_df.pivot_table(
        index=['vcAgentName', 'vcORGName'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum',
        fill_value=0
    ).round(0)
    return pivot_table

def export_to_excel(all_pivots):
    """Export all pivot tables to Excel file"""
    current_date = datetime.now().strftime('%Y%m%d')
    filename = f'RCS_REPORT_{current_date}.xlsx'
    
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        # Sheet names and their corresponding pivots
        sheets = {
            'Traffic Analysis': all_pivots['traffic'],
            'Content Analysis': all_pivots['od'],
            'Daily Volumes': all_pivots['volume'],
            'OD Summary': all_pivots['od_summary'],
            'Traffic by Type': all_pivots['traffic_type'],
            'Daily OD Detail': all_pivots['od_detail'],
            'Traffic Detail': all_pivots['traffic_detail'],
            'Agent Summary': all_pivots['agent_summary'],
            'Agent-Aggregator': all_pivots['agent_aggregator']
        }
        
        # Write all sheets
        for sheet_name, pivot in sheets.items():
            pivot.to_excel(writer, sheet_name=sheet_name, merge_cells=True)
            
            worksheet = writer.sheets[sheet_name]
            workbook = writer.book
            
            # Common formats
            header_format = workbook.add_format({
                'bold': True,
                'fg_color': '#D7E4BC',
                'border': 1,
                'align': 'center'
            })
            
            number_format = workbook.add_format({
                'num_format': '#,##0',
                'align': 'right'
            })
            
            # Format columns
            worksheet.set_column(0, len(pivot.columns) + 1, 15, number_format)
            worksheet.set_column(0, 0, 30)  # First column wider
            
            # Format headers
            for col_num, value in enumerate(pivot.columns.values):
                worksheet.write(0, col_num + 1, str(value), header_format)
    
    return filename

# Load mapping.csv
mappings = pd.read_csv('mapping.csv')

# Update only traffic DataFrames with vcType
for label in ['FTD', 'MTD', 'LMTD']:
    traffic_key = f"{label}_traffic"
    if traffic_key in dfs:
        dfs[traffic_key] = update_df_with_type(dfs[traffic_key], mappings)

# Add volume columns to OD DataFrames
for label in ['FTD', 'MTD', 'LMTD']:
    od_key = f"{label}_OD"
    if od_key in dfs:
        dfs[od_key] = add_volume_columns(dfs[od_key])

# After processing all dataframes, create pivot tables
all_pivots = {
    'traffic': create_comparative_pivot(dfs),
    'od': create_od_pivot(dfs),
    'volume': create_daily_volume_pivot(dfs),
    'od_summary': create_od_summary_pivot(dfs),
    'traffic_type': create_traffic_by_type_pivot(dfs),
    'od_detail': create_daily_od_detail_pivot(dfs),
    'traffic_detail': create_daily_traffic_detail_pivot(dfs),
    'agent_summary': create_traffic_by_agent_pivot(dfs),
    'agent_aggregator': create_agent_aggregator_pivot(dfs)
}

# Export all pivots to Excel
excel_file = export_to_excel(all_pivots)
print(f"Report exported to: {excel_file}")

