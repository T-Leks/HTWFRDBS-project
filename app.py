### <--- ALL I WANT --->
from flask import Flask, render_template, request, session, send_file
import cx_Oracle
from cx_Oracle import DatabaseError
import os
import csv


### <--- CONFIGURE FLASK --->
app = Flask(__name__)
app.secret_key = os.urandom(1235)


### <--- BUILDING WEB-APP TEMPLATES --->
@app.route('/')
def homePage():
    return render_template('home.html')

@app.route('/database_connection')
def connectionFormPage():
    return render_template('database_connection.html')

@app.route('/source_data')
def showSourceDataPage():
    return render_template('source_data.html')

@app.route('/select_columns')
def selectColumnsPage():
    return render_template('select_columns.html')

@app.route('/config_columns')
def configureColumnsPage():
    return render_template('config_columns.html')

@app.route('/target_data')
def showTargetDataPage():
    return render_template('target_data.html')

@app.route('/export_files')
def exportFilesPage():
    return render_template('export.files.html')


### <--- DEFINING FUNCTION --->
@app.route('/connection_form', methods=['GET', 'POST'])
def databaseConnectionForm():
    if request.method == "POST":
        ### <--- Store Source & Target Connection Form --->
        session['SOU_USER'] = request.form['sou_username']
        session['SOU_PASS'] = request.form['sou_password']
        session['SOU_HOST'] = request.form['sou_hostname']
        session['SOU_PORT'] = request.form['sou_port']
        session['SOU_SID'] = request.form['sou_sid']

        session['TAR_USER'] = request.form['tar_username']
        session['TAR_PASS'] = request.form['tar_password']
        session['TAR_HOST'] = request.form['tar_hostname']
        session['TAR_PORT'] = request.form['tar_port']
        session['TAR_SID'] = request.form['tar_sid']
        ### <--------------------------------------------->

        SOU_USER = request.form['sou_username']
        SOU_PASS = request.form['sou_password']
        SOU_DBURL = (request.form['sou_hostname'] + ':' + request.form['sou_port'] + '/' + request.form['sou_sid'])
        TAR_USER = request.form['tar_username']
        TAR_PASS = request.form['tar_password']
        TAR_DBURL = (request.form['tar_hostname'] + ':' + request.form['tar_port'] + '/' + request.form['tar_sid'])

        try:
            SOURCE_CONN = cx_Oracle.connect(SOU_USER, SOU_PASS, SOU_DBURL)
            SOURCE_CUR = SOURCE_CONN.cursor()
            TARGET_CONN = cx_Oracle.connect(TAR_USER, TAR_PASS, TAR_DBURL)
            TARGET_CUR = SOURCE_CONN.cursor()

            GET_TABLE_NAME = " SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = UPPER('"+ SOU_USER.upper() +"') "
            SOURCE_CUR.execute(GET_TABLE_NAME)
            TABLES = SOURCE_CUR.fetchall()
            SOU_TABLE_CUT = []
            for i in range(len(TABLES)):
                TABLE = TABLES[i]
                SOU_TABLE_CUT.append(TABLE[0])

        except cx_Oracle.DatabaseError as e:
            error, = e.args
            #print('Error.code =', error.code)
            #print('Error.message =', error.message)
            #print('Error.offset =', error.offset)
            return render_template('database_connection.html', errors=error.message)

        session['SOU_TABLE_CUT'] = SOU_TABLE_CUT

    return render_template('source_data.html', tables=SOU_TABLE_CUT)

@app.route('/get_data_source', methods=['GET', 'POST'])
def showDataSource():
    if request.method == "POST":
        SOU_USER = session['SOU_USER']
        SOU_PASS = session['SOU_PASS']
        SOU_DBURL = (session['SOU_HOST'] + ':' + session['SOU_PORT'] + '/' + session['SOU_SID'])

        SOU_TABLE_NAME = request.form.get('table_selected')
        session['SOU_TABLE_NAME'] = request.form.get('table_selected')

        SOURCE_CONN = cx_Oracle.connect(SOU_USER, SOU_PASS, SOU_DBURL)
        SOURCE_CUR = SOURCE_CONN.cursor()

        GET_DATA = " SELECT * FROM "+ SOU_TABLE_NAME +" "
        SOURCE_CUR.execute(GET_DATA)
        DATA = SOURCE_CUR.fetchall()
        DATA_CUT = []
        for i in DATA:
            DATA_CUT.append(i)

        GET_COLUMN_NAME = " SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '"+ SOU_TABLE_NAME +"' ORDER BY COLUMN_ID "
        SOURCE_CUR.execute(GET_COLUMN_NAME)
        COLUMNS = SOURCE_CUR.fetchall()
        COL_CUT = []
        for i in range(len(COLUMNS)):
            COL = COLUMNS[i]
            COL_CUT.append(COL[0])
        session['COL_CUT'] = COL_CUT
        SOU_TABLE_CUT = session['SOU_TABLE_CUT']

    return render_template('source_data.html', columns=COL_CUT, data=DATA_CUT, tables=SOU_TABLE_CUT, tbn=SOU_TABLE_NAME)

@app.route('/select_cols', methods=['GET', 'POST'])
def selectColumns():
    COL_CUT = session['COL_CUT']
    return render_template('select_columns.html', columns=COL_CUT)

@app.route('/get_column_details', methods=['GET', 'POST'])
def selectColumnDetails():
    SOU_USER = session['SOU_USER']
    SOU_PASS = session['SOU_PASS']
    SOU_DBURL = (session['SOU_HOST'] +':'+ session['SOU_PORT'] +'/'+ session['SOU_SID'])
    SOU_TABLE_NAME = session['SOU_TABLE_NAME']

    if request.method == "POST":
        COLUMNS = request.form.getlist('col_selected')
        session['COLUMNS'] = request.form.getlist('col_selected')
        COLUMN_NAME = ""
        for i in range(len(COLUMNS)):
            COLUMN_NAME = COLUMN_NAME + "'" + COLUMNS[i] + "'"
            if i < len(COLUMNS) - 1:
                COLUMN_NAME += ","

        SOURCE_CONN = cx_Oracle.connect(SOU_USER, SOU_PASS, SOU_DBURL)
        SOURCE_CUR = SOURCE_CONN.cursor()

        GET_COLUMN_DETAILS = " SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH," \
                             " CASE WHEN DATA_TYPE = 'DATE' THEN 'DATE' " \
                             " ELSE DATA_TYPE||'('||DATA_LENGTH||')' END AS TYPE_LENGTH " \
                             " FROM USER_TAB_COLUMNS " \
                             " WHERE COLUMN_NAME in ("+ COLUMN_NAME +") AND TABLE_NAME = '"+ SOU_TABLE_NAME +"' " \
                             " ORDER BY COLUMN_ID "
        SOURCE_CUR.execute(GET_COLUMN_DETAILS)
        ROWS = SOURCE_CUR.fetchall()
        COLUMN_DETAILS = []
        for i in ROWS:
            COLUMN_DETAILS.append(i)
        session['COLUMN_DETAILS'] = COLUMN_DETAILS

    return render_template('config_columns.html', details=COLUMN_DETAILS)

@app.route('/config_columns', methods=['GET', 'POST'])
def configureColumnsForm():
    SOU_USER = session['SOU_USER']
    SOU_PASS = session['SOU_PASS']
    SOU_DBURL = (session['SOU_HOST'] + ':' + session['SOU_PORT'] + '/' + session['SOU_SID'])
    TAR_USER = session['TAR_USER']
    TAR_PASS = session['TAR_PASS']
    TAR_DBURL = (session['TAR_HOST'] + ':' + session['TAR_PORT'] + '/' + session['TAR_SID'])
    SOU_TABLE_NAME = session['SOU_TABLE_NAME']
    COLUMN_DETAILS = session['COLUMN_DETAILS']
    PICK_COLUMNS = session['COLUMNS']

    if request.method == "POST":
        CHECK_TABLE = request.form.get('check_table')
        TAR_TABLE_NAME = request.form['new_table_name']
        NEW_COLUMN_NAME = request.form.getlist('new_col_name')
        NEW_DATA_TYPE = request.form.getlist('new_data_type')
        NEW_DATA_LENGTH = request.form.getlist('new_data_length')

        SOURCE_CONN = cx_Oracle.connect(SOU_USER, SOU_PASS, SOU_DBURL)
        SOURCE_CUR = SOURCE_CONN.cursor()
        TARGET_CONN = cx_Oracle.connect(TAR_USER, TAR_PASS, TAR_DBURL)
        TARGET_CUR = TARGET_CONN.cursor()

        # <----------------------------------->
        # <--- check if it's a fact table. --->
        # <----------------------------------->
        if CHECK_TABLE == 'Fact':
            # <---------------------------------------------------------------------->
            # <--- check in target database that's already have sequences or not. --->
            # <---------------------------------------------------------------------->
            CHECK_FACT_SEQ = " SELECT COUNT(*) " \
                             " FROM USER_SEQUENCES " \
                             " WHERE SEQUENCE_NAME = 'SEQ_" + TAR_TABLE_NAME.upper() + "' "
            TARGET_CUR.execute(CHECK_FACT_SEQ)
            ROWS = TARGET_CUR.fetchall()
            CHECK_FAS = []
            for i in range(len(ROWS)):
                ROW = ROWS[i]
                CHECK_FAS.append(ROW[0])

            CHECK_DATE_SEQ = " SELECT COUNT(*) " \
                             " FROM USER_SEQUENCES " \
                             " WHERE SEQUENCE_NAME = 'SEQ_DATE_DIMENSION' "
            TARGET_CUR.execute(CHECK_DATE_SEQ)
            ROWS = TARGET_CUR.fetchall()
            CHECK_DAS = []
            for i in range(len(ROWS)):
                ROW = ROWS[i]
                CHECK_DAS.append(ROW[0])

            CHECK_FACT_TABLE = " SELECT COUNT(*) " \
                               " FROM USER_TABLES " \
                               " WHERE TABLE_NAME = '" + TAR_TABLE_NAME.upper() + "' "
            TARGET_CUR.execute(CHECK_FACT_TABLE)
            ROWS = TARGET_CUR.fetchall()
            CHECK_FAT = []
            for i in range(len(ROWS)):
                ROW = ROWS[i]
                CHECK_FAT.append(ROW[0])

            CHECK_DATE_DIM = " SELECT COUNT(*) " \
                             " FROM USER_TABLES " \
                             " WHERE TABLE_NAME = 'DATE_DIMENSION' "
            TARGET_CUR.execute(CHECK_DATE_DIM)
            ROWS = TARGET_CUR.fetchall()
            CHECK_DAD = []
            for i in range(len(ROWS)):
                ROW = ROWS[i]
                CHECK_DAD.append(ROW[0])
            # <---------------------------------------------------------------------->

            if ((CHECK_FAS == [0]) & (CHECK_DAS == [0])) & ((CHECK_FAT == [0]) & (CHECK_DAD == [0])):
                # <------------------------------------------------------------>
                # <--- if in target database not have sequences, create it. --->
                # <------------------------------------------------------------>
                CREATE_FACT_SEQ = " CREATE SEQUENCE "+ TAR_USER.upper() +".SEQ_"+ TAR_TABLE_NAME +" " \
                                  " MINVALUE 1 " \
                                  " START WITH 1 " \
                                  " INCREMENT BY 1 " \
                                  " CACHE 20 "
                SOURCE_CUR.execute(CREATE_FACT_SEQ)

                CREATE_DATE_SEQ = " CREATE SEQUENCE " + TAR_USER.upper() + ".SEQ_DATE_DIMENSION " \
                                  " MINVALUE 1 " \
                                  " START WITH 1 " \
                                  " INCREMENT BY 1 " \
                                  " CACHE 20 "
                SOURCE_CUR.execute(CREATE_DATE_SEQ)
                # <------------------------------------------------------------>

                # <-------------------------->
                # <--- create fact table. --->
                # <-------------------------->
                CFT_CREATE = ""
                for i in range(len(NEW_COLUMN_NAME)):
                    if NEW_DATA_TYPE[i] == 'DATE':
                        CFT_CREATE = CFT_CREATE + " " + NEW_COLUMN_NAME[i] + " " + NEW_DATA_TYPE[i]

                    else:
                        CFT_CREATE = CFT_CREATE + " " + NEW_COLUMN_NAME[i] + " " + NEW_DATA_TYPE[i] + "(" + \
                                            NEW_DATA_LENGTH[i] + ") "

                    if i < len(NEW_COLUMN_NAME) - 1:
                        CFT_CREATE += ","

                CREATE_FACT_TABLE = " CREATE TABLE "+ TAR_USER.upper() +"."+ TAR_TABLE_NAME +" " \
                                    " (SRG_KEY INT, " \
                                    " " + CFT_CREATE + " )"
                SOURCE_CUR.execute(CREATE_FACT_TABLE)
                # <-------------------------->

                # <----------------------------------------------------------->
                # <--- create date dimension from date data in fact table. --->
                # <----------------------------------------------------------->
                for i in COLUMN_DETAILS:
                    if i[1] == 'DATE':
                        CREATE_DATE_DIM = " CREATE TABLE "+ TAR_USER.upper() +".DATE_DIMENSION" \
                                          " AS( SELECT "+ TAR_USER.upper() +".SEQ_DATE_DIMENSION.nextval as \"SRG_KEY\", " \
                                          " TO_CHAR("+ i[0] +", 'DD/MM/YYYY') as FULL_DATE_ARABIC, " \
                                          " TO_CHAR("+ i[0] +", 'DY') as DAY_SHORT, " \
                                          " TO_CHAR("+ i[0] +", 'MM') as MONTH_NUM, " \
                                          " TO_CHAR("+ i[0] +", 'MONTH') as MONTH_NAME, " \
                                          " TO_CHAR("+ i[0] +", 'MON') as MONTH_SHORT, " \
                                          " TO_CHAR("+ i[0] +", 'YYYY') as YEAR, " \
                                          " TO_CHAR(TO_DATE("+ i[0] +", 'DD/MM/RRRR'), 'D') as DAY_OF_WEEK " \
                                          " FROM "+ SOU_USER.upper() +"."+ SOU_TABLE_NAME +") "
                        SOURCE_CUR.execute(CREATE_DATE_DIM)

                    IFT_INSERT_TAR = ""
                    for i in range(len(NEW_COLUMN_NAME)):
                        IFT_INSERT_TAR = IFT_INSERT_TAR + " " + NEW_COLUMN_NAME[i] + " "

                        if i < len(NEW_COLUMN_NAME) - 1:
                            IFT_INSERT_TAR += ","

                    IFT_SELECT_SOU = ""
                    for i in range(len(PICK_COLUMNS)):
                        IFT_SELECT_SOU = IFT_SELECT_SOU + " " + PICK_COLUMNS[i] + " "

                        if i < len(PICK_COLUMNS) - 1:
                            IFT_SELECT_SOU += ","

                    INSERT_FACT_TABLE = " INSERT INTO " + TAR_USER.upper() + "." + TAR_TABLE_NAME + "( SRG_KEY, " + IFT_INSERT_TAR + " ) " \
                                        " ( SELECT " + TAR_USER.upper() + ".SEQ_" + TAR_TABLE_NAME + ".nextval as \"SRG_KEY\"," \
                                        " " + IFT_SELECT_SOU + " " \
                                        " FROM " + SOU_USER.upper() + "." + SOU_TABLE_NAME + " ) "
                    SOURCE_CUR.execute(INSERT_FACT_TABLE)
                    SOURCE_CONN.commit()
                # <----------------------------------------------------------->

            # <-------------------------------------------------------------->
            # <--- merge it, if it's already have table that be the same. --->
            # <-------------------------------------------------------------->
            else:
                MFT_INSERT_TAR = ""
                for i in range(len(NEW_COLUMN_NAME)):
                    MFT_INSERT_TAR = MFT_INSERT_TAR + " TAR." + NEW_COLUMN_NAME[i] + " "

                    if i < len(NEW_COLUMN_NAME) - 1:
                        MFT_INSERT_TAR += ","

                MFT_SELECT_SOU = ""
                MFT_VALUES_SOU = ""
                for i in range(len(PICK_COLUMNS)):
                    MFT_SELECT_SOU = MFT_SELECT_SOU + " " + PICK_COLUMNS[i] + " "
                    MFT_VALUES_SOU = MFT_VALUES_SOU + " SOU." + PICK_COLUMNS[i] + " "

                    if i < len(PICK_COLUMNS) - 1:
                        MFT_SELECT_SOU += ","
                        MFT_VALUES_SOU += ","

                MFT_JOIN = ""
                for i in range(len(NEW_COLUMN_NAME)):
                    for j in range(len(PICK_COLUMNS)):
                        if i == j:
                            MFT_JOIN = MFT_JOIN + " TAR." + NEW_COLUMN_NAME[j] + " = SOU." + PICK_COLUMNS[j] + " "

                    if i < len(NEW_COLUMN_NAME) -1:
                        MFT_JOIN += "AND"

                MERGE_FACT_TABLE = " MERGE INTO " + TAR_USER.upper() + "." + TAR_TABLE_NAME + " TAR " \
                                   " USING (SELECT " + MFT_SELECT_SOU + " FROM " + SOU_USER.upper() + "." + SOU_TABLE_NAME + ") SOU " \
                                   " ON (" + MFT_JOIN + ") " \
                                   " WHEN NOT MATCHED THEN INSERT " \
                                   " (TAR.SRG_KEY, " + MFT_INSERT_TAR + ") " \
                                   " VALUES " \
                                   " (" + TAR_USER.upper() + ".SEQ_" + TAR_TABLE_NAME + ".nextval, " \
                                   " " + MFT_VALUES_SOU + ") "
                SOURCE_CUR.execute(MERGE_FACT_TABLE)

                for i in COLUMN_DETAILS:
                    if i[1] == 'DATE':
                        MERGE_DATE_DIM = " MERGE INTO " + TAR_USER.upper() + ".DATE_DIMENSION TAR " \
                                         " USING (SELECT DISTINCT " + i[0] + " FROM " + SOU_USER.upper() + "." + SOU_TABLE_NAME + ") SOU " \
                                         " ON (TAR.FULL_DATE_ARABIC = TO_CHAR(SOU." + i[0] + ", 'DD/MM/YYYY')) " \
                                         " WHEN NOT MATCHED THEN INSERT " \
                                         " (TAR.SRG_KEY, TAR.FULL_DATE_ARABIC, TAR.DAY_SHORT, TAR.MONTH_NUM, TAR.MONTH_NAME, TAR.MONTH_SHORT, TAR.YEAR, TAR.DAY_OF_WEEK) " \
                                         " VALUES " \
                                         " (" + TAR_USER.upper() + ".SEQ_DATE_DIMENSION.nextval, " \
                                         " TO_CHAR(SOU." + i[0] + ", 'DD/MM/YYYY'), " \
                                         " TO_CHAR(SOU." + i[0] + ", 'DY'), " \
                                         " TO_CHAR(SOU." + i[0] + ", 'MM'), " \
                                         " TO_CHAR(SOU." + i[0] + ", 'MONTH'), " \
                                         " TO_CHAR(SOU." + i[0] + ", 'MON'), " \
                                         " TO_CHAR(SOU." + i[0] + ", 'YYYY'), " \
                                         " TO_CHAR(TO_DATE(SOU." + i[0] + ", 'DD/MM/RRRR'), 'DD')) "
                        SOURCE_CUR.execute(MERGE_DATE_DIM)
                SOURCE_CONN.commit()
            # <-------------------------------------------------------------->

        # <---------------------------------------->
        # <--- check if it's a dimension table. --->
        # <---------------------------------------->
        else:
            # <---------------------------------------------------------------------->
            # <--- check in target database that's already have sequences or not. --->
            # <---------------------------------------------------------------------->
            CHECK_DIM_SEQ = " SELECT COUNT(*) " \
                            " FROM USER_SEQUENCES " \
                            " WHERE SEQUENCE_NAME = 'SEQ_" + TAR_TABLE_NAME.upper() + "' "
            TARGET_CUR.execute(CHECK_DIM_SEQ)
            ROWS = TARGET_CUR.fetchall()
            CHECK_DIS = []
            for i in range(len(ROWS)):
                ROW = ROWS[i]
                CHECK_DIS.append(ROW[0])

            CHECK_DIM_TABLE = " SELECT COUNT(*) " \
                              " FROM USER_TABLES " \
                              " WHERE TABLE_NAME = '" + TAR_TABLE_NAME.upper() + "' "
            TARGET_CUR.execute(CHECK_DIM_TABLE)
            ROWS = TARGET_CUR.fetchall()
            CHECK_DIT = []
            for i in range(len(ROWS)):
                ROW = ROWS[i]
                CHECK_DIT.append(ROW[0])
            # <---------------------------------------------------------------------->

            if (CHECK_DIS == [0]) & (CHECK_DIT == [0]):
                # <------------------------------------------------------------>
                # <--- if in target database not have sequences, create it. --->
                # <------------------------------------------------------------>
                CREATE_DIM_SEQ = " CREATE SEQUENCE " + TAR_USER.upper() + ".SEQ_" + TAR_TABLE_NAME + " " \
                                  " MINVALUE 1 " \
                                  " START WITH 1 " \
                                  " INCREMENT BY 1 " \
                                  " CACHE 20 "
                SOURCE_CUR.execute(CREATE_DIM_SEQ)
                # <------------------------------------------------------------>

                # <------------------------------->
                # <--- create dimension table. --->
                # <------------------------------->
                CDT_CREATE = ""
                for i in range(len(NEW_COLUMN_NAME)):
                    if NEW_DATA_TYPE[i] == 'DATE':
                        CDT_CREATE = CDT_CREATE + " START_DATE " + NEW_DATA_TYPE[i] + " , END_DATE " + NEW_DATA_TYPE[i]

                    else:
                        CDT_CREATE = CDT_CREATE + " " + NEW_COLUMN_NAME[i] + " " + NEW_DATA_TYPE[i] + "(" + NEW_DATA_LENGTH[
                            i] + ") "

                    if i < len(NEW_COLUMN_NAME) - 1:
                        CDT_CREATE += ","

                CREATE_DIM_TABLE = " CREATE TABLE " + TAR_USER.upper() + "." + TAR_TABLE_NAME + " " \
                                   " (SRG_KEY INT, " \
                                   " " + CDT_CREATE + " )"
                SOURCE_CUR.execute(CREATE_DIM_TABLE)
                # <------------------------------->

                # <------------------------------->
                # <--- if not, then insert it. --->
                # <------------------------------->
                IDT_INSERT_TAR = ""

                for i in range(len(NEW_COLUMN_NAME)):
                    if NEW_DATA_TYPE[i] != 'DATE':
                        IDT_INSERT_TAR = IDT_INSERT_TAR + " TAR." + NEW_COLUMN_NAME[i]

                    if ((i < len(NEW_COLUMN_NAME) - 1) & (NEW_DATA_TYPE[i] != 'DATE')) & \
                            ~((i == len(NEW_COLUMN_NAME) - 2) & (NEW_DATA_TYPE[len(NEW_COLUMN_NAME) - 1] == 'DATE')):
                        IDT_INSERT_TAR += ","

                IDT_SELECT_SOU = ""
                IDT_GROUPBY_SOU = ""
                IDT_VALUES_SOU = ""

                for i in range(len(PICK_COLUMNS)):
                    if NEW_DATA_TYPE[i] == 'DATE':
                        IDT_SELECT_SOU = IDT_SELECT_SOU + " MIN(" + PICK_COLUMNS[i] + ") as START_DATE, MAX(" + \
                                         PICK_COLUMNS[i] + ") as END_DATE "

                    else:
                        IDT_GROUPBY_SOU = IDT_GROUPBY_SOU + " " + PICK_COLUMNS[i]
                        IDT_SELECT_SOU = IDT_SELECT_SOU + " " + PICK_COLUMNS[i]
                        IDT_VALUES_SOU = IDT_VALUES_SOU + " SOU." + PICK_COLUMNS[i]

                    if i < len(PICK_COLUMNS) - 1:
                        IDT_SELECT_SOU += ","
                    if ((i < len(PICK_COLUMNS) - 1) & (PICK_COLUMNS[i] != 'DATE')) & \
                            ~((i == len(PICK_COLUMNS) - 2) & (NEW_DATA_TYPE[len(PICK_COLUMNS) - 1] == 'DATE')):
                        IDT_GROUPBY_SOU += ","
                        IDT_VALUES_SOU += ","

                IDT_JOIN = ""
                for i in range(len(NEW_COLUMN_NAME)):
                    for j in range(len(PICK_COLUMNS)):
                        if (i == j) & (NEW_DATA_TYPE[i] != 'DATE'):
                            IDT_JOIN = IDT_JOIN + " TAR." + NEW_COLUMN_NAME[j] + " = SOU." + PICK_COLUMNS[j] + " "

                    if ((i < len(PICK_COLUMNS) - 1) & (PICK_COLUMNS[i] != 'DATE')) & \
                            ~((i == len(PICK_COLUMNS) - 2) & (NEW_DATA_TYPE[len(PICK_COLUMNS) - 1] == 'DATE')):
                        IDT_JOIN += "AND"

                INSERT_DIM_TABLE = " MERGE INTO " + TAR_USER.upper() + "." + TAR_TABLE_NAME + " TAR " \
                                   " USING (SELECT " + IDT_SELECT_SOU + " FROM " + SOU_USER.upper() + "." + SOU_TABLE_NAME + " GROUP BY " + IDT_GROUPBY_SOU + ") SOU " \
                                   " ON (" + IDT_JOIN + ") " \
                                   " WHEN NOT MATCHED THEN INSERT (TAR.SRG_KEY," + IDT_INSERT_TAR + ", TAR.START_DATE, TAR.END_DATE) " \
                                   " VALUES " \
                                   " (" + TAR_USER.upper() + ".SEQ_" + TAR_TABLE_NAME + ".nextval, " \
                                   " " + IDT_VALUES_SOU + ", SOU.START_DATE, SOU.END_DATE) "
                SOURCE_CUR.execute(INSERT_DIM_TABLE)
                SOURCE_CONN.commit()
                # <------------------------------->

            # <------------------------------------------>
            # <--- if already have it, then merge it. --->
            # <------------------------------------------>
            else:
                MDT_INSERT_TAR = ""

                for i in range(len(NEW_COLUMN_NAME)):
                    if NEW_DATA_TYPE[i] != 'DATE':
                        MDT_INSERT_TAR = MDT_INSERT_TAR + " TAR." + NEW_COLUMN_NAME[i]

                    if ((i < len(NEW_COLUMN_NAME) - 1) & (NEW_DATA_TYPE[i] != 'DATE')) & \
                            ~((i == len(NEW_COLUMN_NAME) - 2) & (NEW_DATA_TYPE[len(NEW_COLUMN_NAME) - 1] == 'DATE')):
                        MDT_INSERT_TAR += ","

                MDT_SELECT_SOU = ""
                MDT_GROUPBY_SOU = ""
                MDT_VALUES_SOU = ""

                for i in range(len(PICK_COLUMNS)):
                    if NEW_DATA_TYPE[i] == 'DATE':
                        MDT_SELECT_SOU = MDT_SELECT_SOU + " MIN(" + PICK_COLUMNS[i] + ") as START_DATE, MAX(" + PICK_COLUMNS[i] + ") as END_DATE "

                    else:
                        MDT_GROUPBY_SOU = MDT_GROUPBY_SOU + " " + PICK_COLUMNS[i]
                        MDT_SELECT_SOU = MDT_SELECT_SOU + " " + PICK_COLUMNS[i]
                        MDT_VALUES_SOU = MDT_VALUES_SOU + " SOU." + PICK_COLUMNS[i]

                    if i < len(PICK_COLUMNS) - 1:
                        MDT_SELECT_SOU += ","
                    if ((i < len(PICK_COLUMNS) - 1) & (PICK_COLUMNS[i] != 'DATE')) & \
                            ~((i == len(PICK_COLUMNS) - 2) & (NEW_DATA_TYPE[len(PICK_COLUMNS) - 1] == 'DATE')):
                        MDT_GROUPBY_SOU += ","
                        MDT_VALUES_SOU += ","

                MDT_JOIN = ""
                for i in range(len(NEW_COLUMN_NAME)):
                    for j in range(len(PICK_COLUMNS)):
                        if (i == j) & (NEW_DATA_TYPE[i] == 'DATE'):
                            MDT_JOIN = MDT_JOIN + " TAR.START_DATE = SOU.START_DATE AND TAR.END_DATE = SOU.END_DATE"
                        elif (i == j) & (NEW_DATA_TYPE[i] != 'DATE'):
                            MDT_JOIN = MDT_JOIN + " TAR." + NEW_COLUMN_NAME[j] + " = SOU." + PICK_COLUMNS[j] + " "

                    if i < len(NEW_COLUMN_NAME) - 1:
                        MDT_JOIN += "AND"

                MERGE_DIM_TABLE = " MERGE INTO " + TAR_USER.upper() + "." + TAR_TABLE_NAME + " TAR " \
                                  " USING (SELECT " + MDT_SELECT_SOU + " FROM " + SOU_USER.upper() + "." + SOU_TABLE_NAME + " GROUP BY " + MDT_GROUPBY_SOU + ") SOU " \
                                  " ON (" + MDT_JOIN + ") " \
                                  " WHEN NOT MATCHED THEN INSERT (TAR.SRG_KEY," + MDT_INSERT_TAR + ", TAR.START_DATE, TAR.END_DATE) " \
                                  " VALUES " \
                                  " (" + TAR_USER.upper() + ".SEQ_" + TAR_TABLE_NAME + ".nextval, " \
                                  " " + MDT_VALUES_SOU + ", SOU.START_DATE, SOU.END_DATE) "
                SOURCE_CUR.execute(MERGE_DIM_TABLE)
                SOURCE_CONN.commit()
            # <------------------------------------------>

        GET_TABLE_NAME = " SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = UPPER('" + TAR_USER.upper() + "') "
        TARGET_CUR.execute(GET_TABLE_NAME)
        TABLES = TARGET_CUR.fetchall()
        TAR_TABLE_CUT = []
        for i in range(len(TABLES)):
            TABLE = TABLES[i]
            TAR_TABLE_CUT.append(TABLE[0])
        session['TAR_TABLE_CUT'] = TAR_TABLE_CUT

    return render_template('target_data.html', tables=TAR_TABLE_CUT)#, stats=STAT_DICT)

# @app.route('/get_target_table')
# def getTargetTableName():
#     TAR_USER = session['TAR_USER']
#     TAR_PASS = session['TAR_PASS']
#     TAR_DBURL = (session['TAR_HOST'] + ':' + session['TAR_PORT'] + '/' + session['TAR_SID'])
#
#     TARGET_CONN = cx_Oracle.connect(TAR_USER, TAR_PASS, TAR_DBURL)
#     TARGET_CUR = TARGET_CONN.cursor()
#
#     GET_TABLE_NAME = " SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = UPPER('" + TAR_USER.upper() + "') "
#     TARGET_CUR.execute(GET_TABLE_NAME)
#     TABLES = TARGET_CUR.fetchall()
#     TAR_TABLE_CUT = []
#     for i in range(len(TABLES)):
#         TABLE = TABLES[i]
#         TAR_TABLE_CUT.append(TABLE[0])
#     session['TAR_TABLE_CUT'] = TAR_TABLE_CUT
#
#     return render_template('target_data.html', tables=TAR_TABLE_CUT)

@app.route('/get_data_target', methods=['GET', 'POST'])
def showDataTarget():
    TAR_USER = session['TAR_USER']
    TAR_PASS = session['TAR_PASS']
    TAR_DBURL = (session['TAR_HOST'] + ':' + session['TAR_PORT'] + '/' + session['TAR_SID'])

    if request.method == "POST":
        TAR_TABLE_NAME = request.form.get('table_selected')
        session['TTN'] = TAR_TABLE_NAME

        TARGET_CONN = cx_Oracle.connect(TAR_USER, TAR_PASS, TAR_DBURL)
        TARGET_CUR = TARGET_CONN.cursor()

        GET_DATA = " SELECT * FROM " + TAR_TABLE_NAME + " "
        TARGET_CUR.execute(GET_DATA)
        DATA = TARGET_CUR.fetchall()
        DATA_CUT = []
        for i in DATA:
            DATA_CUT.append(i)

        GET_COLUMN_NAME = " SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + TAR_TABLE_NAME + "' ORDER BY COLUMN_ID "
        TARGET_CUR.execute(GET_COLUMN_NAME)
        COLUMNS = TARGET_CUR.fetchall()
        COL_CUT = []
        for i in range(len(COLUMNS)):
            COL = COLUMNS[i]
            COL_CUT.append(COL[0])

        TAR_TABLE_CUT = session['TAR_TABLE_CUT']

    return render_template('target_data.html', columns=COL_CUT, data=DATA_CUT, tables=TAR_TABLE_CUT, tbn=TAR_TABLE_NAME)

@app.route('/export_csv_file')
def exportCSVfile():
    TAR_USER = session['TAR_USER']
    TAR_PASS = session['TAR_PASS']
    TAR_DBURL = (session['TAR_HOST'] + ':' + session['TAR_PORT'] + '/' + session['TAR_SID'])
    TTN = session['TTN']

    TARGET_CONN = cx_Oracle.connect(TAR_USER, TAR_PASS, TAR_DBURL)
    TARGET_CUR = TARGET_CONN.cursor()

    SELECT_COLUMN_EXPORT = " SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + TTN + "' ORDER BY COLUMN_ID "
    TARGET_CUR.execute(SELECT_COLUMN_EXPORT)
    COLUMNS = TARGET_CUR.fetchall()
    COL_CUT = []
    for i in range(len(COLUMNS)):
        COL = COLUMNS[i]
        COL_CUT.append(COL[0])

    SELECT_TABLE_EXPORT = " SELECT * FROM "+ TTN.upper() +" "
    TARGET_CUR.execute(SELECT_TABLE_EXPORT)
    DATA = TARGET_CUR.fetchall()

    FILENAME = "" + TTN + ".csv"
    CSV_FILE = open(FILENAME, 'w', newline='')
    if DATA:
        WRITER = csv.writer(CSV_FILE)
        WRITER.writerow(COL_CUT)
        WRITER.writerows(DATA)
    session['FILENAME'] = FILENAME

    return render_template('export_files.html', filename=FILENAME)

@app.route('/download_file', methods=['GET', 'POST'])
def downloadFile():
    if request.method == 'POST':
        FILENAME = session['FILENAME']
        testfile = '../PROJECT-HTWRDS/'+ FILENAME
        return send_file(testfile, as_attachment=True, mimetype='text/csv')


# <--- RUN WEB-APP --->
if __name__ == '__main__':
    app.run(debug=True)