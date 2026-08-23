import sqlite3
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
import datetime

# Configuration
DB_FILE = "ollama_cluster.db"
OUTPUT_FILE = f"Ollama_Cluster_Report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

def export_database():
    try:
        # 1. Extract data from SQLite using Pandas
        with sqlite3.connect(DB_FILE) as conn:
            df_nodes = pd.read_sql_query("SELECT ip as 'IP Address', last_seen as 'Last Seen' FROM nodes", conn)
            df_models = pd.read_sql_query("SELECT node_ip as 'Node IP', model_name as 'Model Name' FROM models", conn)
            
        # Optional: Add a calculated Status column based on recent activity
        df_nodes['Status'] = 'Active'

        # 2. Create a new workbook and remove the default sheet
        wb = openpyxl.Workbook()
        wb.remove(wb.active) 

        # Add Worksheets
        ws_nodes = wb.create_sheet(title="Active Nodes")
        ws_models = wb.create_sheet(title="Available Models")

        # 3. Styling Definitions
        header_fill = PatternFill(start_color="2C3E50", end_color="2C3E50", fill_type="solid") # Dark Slate
        header_font = Font(color="FFFFFF", bold=True)
        border_side = Side(border_style="thin", color="D3D3D3")
        thin_border = Border(left=border_side, right=border_side, top=border_side, bottom=border_side)
        alt_fill = PatternFill(start_color="F8F9F9", end_color="F8F9F9", fill_type="solid") # Very light grey

        def format_sheet(ws, df):
            # Write dataframe to worksheet
            for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
                for c_idx, value in enumerate(row, 1):
                    cell = ws.cell(row=r_idx, column=c_idx, value=value)
                    
                    # Header styling
                    if r_idx == 1:
                        cell.fill = header_fill
                        cell.font = header_font
                        cell.alignment = Alignment(horizontal="center", vertical="center")
                    else:
                        # Alternate row coloring (Zebra striping)
                        if r_idx % 2 == 0:
                            cell.fill = alt_fill
                        cell.alignment = Alignment(horizontal="left", vertical="center")
                    
                    cell.border = thin_border

            # Adjust column widths & Add Auto-Filters
            for col in ws.columns:
                max_length = 0
                column = col[0].column_letter
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 4)
                ws.column_dimensions[column].width = adjusted_width
            
            # Freeze the top header row
            ws.freeze_panes = 'A2'
            # Apply filters to the header row
            ws.auto_filter.ref = ws.dimensions

        # 4. Apply formatting to both sheets
        format_sheet(ws_nodes, df_nodes)
        format_sheet(ws_models, df_models)

        # 5. Save the file
        wb.save(OUTPUT_FILE)
        print(f"[+] Successfully exported cluster database to {OUTPUT_FILE}")

    except Exception as e:
        print(f"[-] Error exporting database: {e}")

if __name__ == "__main__":
    export_database()