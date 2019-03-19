import ibm_db
import datetime
import os
import mskcc
import csv
import xlsxwriter
import pypyodbc

###########################
#       CONNECTION        #
###########################

input_file_1 = '../properties.txt'
f_in = open(input_file_1, 'r')
properties_dict = {}
for line in f_in:
    properties_dict[line.partition('=')[0]] = line.partition('=')[2].strip()
f_in.close()

connection_idb = ibm_db.connect('DATABASE=DB2P_MF;'
                     'HOSTNAME=ibm3270;'
                     'PORT=3021;'
                     'PROTOCOL=TCPIP;'
                     'UID={0};'.format(properties_dict["idb_service_uid1"]) +
                     'PWD={0};'.format(mskcc.decrypt(properties_dict["idb_service_pwd1"]).decode("latin-1")), '', '')

connection_darwin = ibm_db.connect('DATABASE=DVPDB01;'
                     'HOSTNAME=pidvudb1di1vipdb01;'
                     'PORT=51013;'
                     'PROTOCOL=TCPIP;'
                     'UID={0};'.format(properties_dict["darwin_uid"]) +
                     'PWD={0};'.format(mskcc.decrypt(properties_dict["darwin_pwd"]).decode("latin-1")), '', '')

connection_sql_server = pypyodbc.connect("Driver={{SQL Server}};Server={};Database={};Uid={};Pwd={};".format(
                    "PS23A,61692",
                    "DEDGPDLR2D2",
                    properties_dict["sqlserver_ps23a_uid"],
                    mskcc.decrypt(properties_dict["sqlserver_ps23a_pwd"]).decode("latin-1")
                    )
                )

###########################
#         DECLARE         #
###########################

now_raw = datetime.datetime.now()
now = now_raw.strftime('%Y%m%d-%H%M%S')
today = now_raw.strftime('%Y-%m-%d')
today_mm_dd_yyyy = now_raw.strftime('%m/%d/%Y')
dataline_report_number = os.path.basename(__file__).replace(".py", "")

# file vars
input_file_1 = ""
output_file_1 = ""

# Excel vars
col_widths = []

###########################
#        FUNCTIONS        #
###########################

def output_excel_column_headers_list(worksheet, in_list, row, col_start):
  fmt = workbook.add_format({'bold': True, 'bg_color': '#C5D9F1'})
  d=col_start
  #col_widths = [0 for n in range(0, len(in_list))]
  for n in range(0, len(in_list)):
    col_widths.append(len(in_list[n])+3)
    worksheet.write(row, d+n, in_list[n], fmt)
    worksheet.set_column(d+n, d+n, col_widths[n])

def output_excel_list_width_calc(worksheet, in_list, row):
  for col, cell in enumerate(in_list):
    #print("col_widths[col]: {}, len(cell): {}".format(col_widths[col], len(cell)))
    if isinstance(cell, datetime.date):
      worksheet.write(row, col, cell.strftime('%Y-%m-%d'))
      if len(str(cell)) > col_widths[col]:
        col_widths[col] = len(str(cell))
    if isinstance(cell, datetime.datetime):
      worksheet.write(row, col, cell.strftime('%Y-%m-%d %I:%M %p'))
      if len(str(cell)) > col_widths[col]:
        col_widths[col] = len(str(cell))
    elif isinstance(cell, str):
      worksheet.write(row, col, cell.strip())
      if len(cell.strip()) > col_widths[col]:
        col_widths[col] = len(cell.strip())
    elif isinstance(cell, int):
      worksheet.write(row, col, cell)
      if len(str(cell)) > col_widths[col]:
        col_widths[col] = len(str(cell))
    else:
      worksheet.write(row, col, cell)
      if len(cell) > col_widths[col]:
        col_widths[col] = len(cell)
  for col, width in enumerate(col_widths):
    worksheet.set_column(col, col, width+3)
  return 0

def row_to_dict(row_raw, columns):
  row = {}
  x = 0
  for col in columns:
      row[col] = row_raw[x]
      x += 1
  return row

def get_recipients(dataline_report_number):
  recipient_list = []
  SQL = """
    select recipient + '@mskcc.org' recipient
    from dbo.scheduler 
    join dbo.scheduler_recipients on scheduler_id=id
    where enabled=1 and project_code = '{}'
  """.format(dataline_report_number)

  cursor = connection_sql_server.cursor()
  cursor.execute(SQL)

  row = {}
  row_raw = cursor.fetchone()
  while row_raw is not None:
      columns = [column[0] for column in cursor.description]
      row = row_to_dict(row_raw, columns)

      recipient_list.append(row["recipient"])
      row_raw = cursor.fetchone()

  cursor.close()
  return recipient_list

###########################
#          MAIN           #
###########################

report_list = [
    
    ("M10 Daily Discharge Report", "'NS10'", "zzPDL_HAD_M10_Discharge_Report@mskcc.org"),
    ("M19 Daily Discharge Report", "'NS19'", "zzPDL_HAD_M19_DischargeReport@mskcc.org"),
    ("M18 Daily Discharge Report", "'NS18'", "zzPDL_HAD_M18_DischargeReport@mskcc.org"),
    ("M17 Daily Discharge Report", "'NS17'", "zzPDL_HAD_M17_DischargeReport@mskcc.org;HosseinN@mskcc.org"),
    ("M16 Daily Discharge Report", "'NS16'", "zzPDL_HAD_M16_DischargeReport@mskcc.org"),    
    ("M15 Daily Discharge Report", "'NS15'", "zzPDL_HAD_M15_DischargeReport@mskcc.org"),
    ("M14 Daily Discharge Report", "'NS14", "zzPDL_HAD_M14_Discharge_Report@mskcc.org;zzPDL_NUR_M14_ChargeRN@mskcc.org"),
    ("M12 Daily Discharge Report", "'NS12'", "zzPDL_HAD_M12_Discharge_Report@mskcc.org;HosseinN@mskcc.org"),
    ("M09/PICU Daily Discharge Report", "'NS-9', 'PICU', 'UCCP'", "zzPDL_PED_M9_ChargeRNs@mskcc.org;zzPDL_PED_UnitAssts@mskcc.org;zzPDL_PED_M9OpEx@mskcc.org"),
    ("M08 Daily Discharge Report", "'NS-8'", "zzPDL_HAD_M08_Discharge_Report@mskcc.org;HosseinN@mskcc.org;zzPDL_M8_NPs_PAs@mskcc.org"),
    ("M7 Daily Discharge Report" , "'NS-7", "zzPDL_M8_NPs_PAs@mskcc.org;zzPDl_NUR_m7_24871@mskcc.org;zzPDL_NUR_M8_IDPTeam@mskcc.org;dowlingm@mskcc.org"),
    ("M05 Daily Discharge Report", "'NS-5'", "zzPDL_HAD_M05_Discharge_Report@mskcc.org;HosseinN@mskcc.org"),
    ("M04 Daily Discharge Report", "'NS-4', 'NACU'", "zzPDL_HAD_M04_Discharge_Report@mskcc.org;HosseinN@mskcc.org")
    
    ]

station_to_report = {
    "NS19": ("M19 Daily Discharge Report (HA17493)", "'NS19'", "zzPDL_HAD_M19_DischargeReport@mskcc.org"),
    "NS18": ("M18 Daily Discharge Report (HA17493)", "'NS18'", "zzPDL_HAD_M18_DischargeReport@mskcc.org"),
    "NS17": ("M17 Daily Discharge Report (HA17493)", "'NS17'", "zzPDL_HAD_M17_Discharge_Report@mskcc.org;HosseinN@mskcc.org"),
    "NS16": ("M16 Daily Discharge Report (HA17493)", "'NS16'", "zzPDL_HAD_M16_DischargeReport@mskcc.org"),
    "NS15": ("M15 Daily Discharge Report (HA17493)", "'NS15'", "zzPDL_HAD_M15_DischargeReport@mskcc.org"),
    "NS14": ("M14 Daily Discharge Report (HA17493)", "'NS14'", "zzPDL_HAD_M14_Discharge_Report@mskcc.org;zzPDL_NUR_M14_ChargeRN@mskcc.org"),
    "NS12": ("M12 Daily Discharge Report (HA17493)", "'NS12'", "zzPDL_HAD_M12_Discharge_Report@mskcc.org;HosseinN@mskcc.org"),
    "NS10": ("M10 Daily Discharge Report (HA17493)", "'NS10'", "zzPDL_HAD_M10_Discharge_Report@mskcc.org"),
    
    "NS-9": ("M09/PICU Daily Discharge Report (HA17493)", "'NS-9', 'PICU', 'UCCP'", "zzPDL_PED_M9_ChargeRNs@mskcc.org;zzPDL_PED_UnitAssts@mskcc.org;zzPDL_PED_M9OpEx@mskcc.org"),
    "PICU": ("M09/PICU Daily Discharge Report (HA17493)", "'NS-9', 'PICU', 'UCCP'", "zzPDL_PED_M9_ChargeRNs@mskcc.org;zzPDL_PED_UnitAssts@mskcc.org;zzPDL_PED_M9OpEx@mskcc.org"),
    "UCCP": ("M09/PICU Daily Discharge Report (HA17493)", "'NS-9', 'PICU', 'UCCP'", "zzPDL_PED_M9_ChargeRNs@mskcc.org;zzPDL_PED_UnitAssts@mskcc.org;zzPDL_PED_M9OpEx@mskcc.org"),
    
    "NS-8": ("M08 Daily Discharge Report (HA17493)", "'NS-8'", "zzPDL_HAD_M08_Discharge_Report@mskcc.org;HosseinN@mskcc.org;zzPDL_M8_NPs_PAs@mskcc.org"),
    "NS-7": ("M7 Daily Discharge Report (HA17493)" , "'NS-7'", "zzPDL_M8_NPs_PAs@mskcc.org;zzPDl_NUR_m7_24871@mskcc.org;zzPDL_NUR_M8_IDPTeam@mskcc.org;dowlingm@mskcc.org"),
    "NS-5": ("M05 Daily Discharge Report (HA17493)", "'NS-5'", "zzPDL_HAD_M05_Discharge_Report@mskcc.org;HosseinN@mskcc.org"),
    "NS-4": ("M04 Daily Discharge Report (HA17493)", "'NS-4', 'NACU'", "zzPDL_HAD_M04_Discharge_Report@mskcc.org;HosseinN@mskcc.org"),
    "NACU": ("M04 Daily Discharge Report (HA17493)", "'NS-4', 'NACU'", "zzPDL_HAD_M04_Discharge_Report@mskcc.org;HosseinN@mskcc.org")
    }

email_data = {}

sql_string = """

select distinct * from (
        SELECT   TRIM(VIS_NURS_STA) as Station,
                 --VIS_ADM_NUM,
                 --VIS_SMS_ACCT_NO,
                 VIS_BED as BED,
                 trim(VIS_HOSP_SVC) as SERVICE,
                 VIS_MRN MRN,
                 trim(PT_FIRST_NAME) as "First Name",
                 trim(PT_LAST_NAME) as "Last Name",
                 trim(DOC_NAME) as Attending,

                 char(Date(OO_ENTER_DT)) as "Order Date",
                 char(Time(OO_ENTER_DT)) as "Order Time",

                 char(VIS_DSCH_DTE) as "Discharge Date",
                 char(VIS_DSCH_TIME) as "Discharge Time",

                 TIMESTAMPDIFF(4, CHAR(TIMESTAMP(VIS_DSCH_DTE,VIS_DSCH_TIME) -OO_ENTER_DT)) as "Order to Discharge Minutes",
                 VIS_DAYS_STAY as "Length of Stay",

                 
                 row_number() over (partition by VIS_ADM_NUM order by (timestamp(VIS_DSCH_DTE, VIS_DSCH_TIME) - oo_enter_DT) ASC)as RN/*,
                 case 
                      when VIS_DSCH_DISP like 'A%' then 'Alive' 
                      when VIS_DSCH_DISP like 'D%' then 'Expired' 
                      when VIS_DSCH_DISP like 'C%' then 'Expired' 
                 END as Status_DC */
        FROM     idb.visit 
                 left join idb.patient on VIS_MRN=PT_MRN 
                 left join IDB.OMS_ORDER on VIS_ADM_NUM = OO_ADM_NUM and OO_MRN = VIS_MRN and (left(UPPER(oo_ord_name),19)='DISCHARGE PATIENT ') and UPPER(OO_ORD_STS_CD) in ('AUC1' ,'AUC3' , 'COMP', 'AUA2') 
                 left join idb.doc on VIS_ATN_DR_NO = doc_DR_NO
        WHERE    VIS_DSCH_dte = CURRENT DATE-1 day
        AND      VIS_VISIT_STATUS IN ('2', '3')
        AND      VIS_IP_OP_ADJUSTED_IND = 'I'
        AND      trim(VIS_NURS_STA) in ('NS19','NS18','NS17','NS16','NS15','NS14','NS12','NS10','NS-9','PICU','UCCP','NS-8','NS-7','NS-5','NS-4','NACU')
        AND      VIS_DSCH_DISP like 'A%'
) a where RN=1
ORDER BY 11

    """.format()

stmt = ibm_db.prepare(connection_darwin, sql_string)

print(sql_string)

ibm_db.execute(stmt)

db_dict = ibm_db.fetch_tuple(stmt)

while db_dict != False:

    station = db_dict[0] # STATION
    report_name = station_to_report[station][0]
    
    if report_name not in email_data:
        email_to = station_to_report[station][2]
        email_body = "<style>table, td { border-collapse: collapse; border: 1px solid black; margin: auto; text-align: center; }</style>"
        #email_body += "This report would go to: {}.<br><br>".format(station_to_report[station][2])
        email_body += """<table style="width:85%;">
                        <tr style="background:#528AE7;font-family:Tahoma;color:white;font-size: 11.0pt;"><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
                      """.format("Station", "Bed", "Service", "MRN", "First Name", "Last Name", "Attending", "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Order Date&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;", "Order Time", "Discharge Date", "Discharge Time", "Order to Discharge Minutes", "Length of Stay")
        email_subject = report_name
        email_data[report_name] = [email_to, email_subject, email_body, [], []]
        
    # list of times to calculate average order time
    if db_dict[7] and db_dict[8]:
        email_data[report_name][3].append(datetime.datetime.strptime("{}-{}".format(db_dict[7], db_dict[8]), '%Y-%m-%d-%H.%M.%S') - now_raw)

    # list of times to calculate average discharge time
    if db_dict[9] and db_dict[10]:
        email_data[report_name][4].append(datetime.datetime.strptime("{}-{}".format(db_dict[9], db_dict[10]), '%Y-%m-%d-%H.%M.%S') - now_raw)

    db_list = list(db_dict)

    if db_list[8]:
        db_list[8] = db_list[8].replace(".", ":")[0:-3]

    if db_list[10]:
        db_list[10] = db_list[10].replace(".", ":")[0:-3]

    email_data[report_name][2] += """<tr style="font-family:Tahoma;font-size: 11.0pt;"><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>""".format(*db_list)

    db_dict = ibm_db.fetch_tuple(stmt)

for report_name in email_data:
    email_to, email_subject, email_body, order_times, discharge_times = email_data[report_name]

    #if 'M14' not in email_subject and 'M16' not in email_subject and 'M12' not in email_subject and 'M04' not in email_subject and 'M18' not in email_subject and 'M17' not in email_subject and 'M09' not in email_subject and 'M05' not in email_subject and 'M15' not in email_subject and 'M10' not in email_subject:
    email_body += "</table><br>"
    
    if len(order_times)  > 0:
        avg_order_time = sum(order_times, datetime.timedelta()) / len(order_times)
        email_body += "<br>Average Order Time: {}".format(datetime.datetime.strftime(avg_order_time + now_raw, '%H:%M'))

    if len(discharge_times) > 0:
        avg_discharge_time = sum(discharge_times, datetime.timedelta()) / len(discharge_times)        
        email_body += "<br>Average Discharge Time: {}".format(datetime.datetime.strftime(avg_discharge_time + now_raw, '%H:%M'))

    #email_to = 'singerm@mskcc.org';
    #today='2019-02-26';#hosseinn@mskcc.org;
    email_subject = "{} - {}".format(email_subject, today)

    sql_string = """
          select DV.SENDJAVAXMAIL('Data/Information Systems <data@mskcc.org>','singerm@mskcc.org;{}','','','{}','{}','text/html;charset=utf-8')
          from SYSIBM.SYSDUMMY1
    """.format(email_to, email_subject, email_body.replace("'", "''").replace('\n', '').replace('\r', ''))
        
    print(sql_string)
        
    stmt_ = ibm_db.prepare(connection_darwin, sql_string)

    ibm_db.execute(stmt_)

    db_dict_ = ibm_db.fetch_tuple(stmt_)

    if db_dict_ != False:
        db_dict_ = ibm_db.fetch_tuple(stmt_)

            

        


