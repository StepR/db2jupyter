
# coding: utf-8

# In[ ]:


get_ipython().magic(u'sql connect')


# # DB2 Jupyter Notebook Extensions
# 
# This code is imported as a Jupyter notebook extension in any notebooks you create with DB2 code in it. Place the following line of code in any notebook that you want to use these commands with:
# <pre>
# &#37;run db2.ipynb
# </pre>
# 
# This code defines a Jupyter/Python magic command called %sql which allows you to execute DB2 specific calls to 
# the database. There are other packages available for manipulating databases, but this one has been specifically
# designed for demonstrating a number of the SQL features available in DB2.
# 
# There are two ways of executing the %sql command. A single line SQL statement would use the
# line format of the magic command:
# <pre>
# %sql SELECT * FROM EMPLOYEE
# </pre>
# If you have a large block of sql then you would place the %%sql command at the beginning of the block and then
# place the SQL statements into the remainder of the block. Using this form of the %%sql statement means that the
# notebook cell can only contain SQL and no other statements.
# <pre>
# %%sql
# SELECT * FROM EMPLOYEE
# ORDER BY LASTNAME
# </pre>
# You can have multiple lines in the SQL block (%%sql). The default SQL delimiter is the semi-column (;).
# If you have scripts (triggers, procedures, functions) that use the semi-colon as part of the script, you 
# will need to use the -d option to change the delimiter to an at "@" sign. 
# <pre>
# %%sql -d
# SELECT * FROM EMPLOYEE
# @
# CREATE PROCEDURE ...
# @
# </pre>
# 
# The %sql command allows most DB2 commands to execute and has a special version of the CONNECT statement. 
# A CONNECT by itself will attempt to reconnect to the database using previously used settings. If it cannot 
# connect, it will prompt the user for additional information. 
# 
# The CONNECT command has the following format:
# <pre>
# %sql CONNECT TO &lt;database&gt; USER &lt;userid&gt; USING &lt;password | ?&gt; HOST &lt;ip address&gt; PORT &lt;port number&gt;
# </pre>
# If you use a "?" for the password field, the system will prompt you for a password. This avoids typing the 
# password as clear text on the screen. If a connection is not successful, the system will print the error
# message associated with the connect request.
# 
# If the connection is successful, the parameters are saved on your system and will be used the next time you
# run a SQL statement, or when you issue the %sql CONNECT command with no parameters.
# 
# In addition to the -d option, there are a number different options that you can specify at the beginning of 
# the SQL:
# 
# - -d - Delimiter: Change SQL delimiter to "@" from ";"
# - -q - Quiet: Quiet results - no answer set or messages returned from the function
# - -n - No result set: Execute all of the SQL as commands rather than select statements (no answer sets) 
# - -s - SQL: Execute everything as SELECT statements. By default, SELECT, VALUES, and WITH are considered part of an answer set, but it is possible that you have an SQL statement that does not start with any of these keywords but returns an answer set.
# - -r - Return the result set as an array of values instead of a dataframe
# - -t - Time: Time the following SQL statement and return the number of times it executes in 1 second
# - -j - JSON: Create a pretty JSON representation. Only the first column is formatted
# - -a - All: Return all rows in answer set and do not limit display
# - -pb - Plot Bar: Plot the results as a bar chart
# - -pl - Plot Line: Plot the results as a line chart
# - -pp - Plot Pie: Plot the results as a pie chart
# - -sampledata - Create and load the EMPLOYEE and DEPARTMENT tables
# 
# One final note. You can pass python variables to the %sql command by using the \{\} braces with the name of the
# variable inbetween. Note that you will need to place proper punctuation around the variable in the event the
# SQL command requires it. For instance, the following example will find employee '000010' in the EMPLOYEE table.
# <pre>
# empno = '000010'
# %sql SELECT LASTNAME FROM EMPLOYEE WHERE EMPNO='{empno}'

# ### Install Db2 Python Driver
# In the event that you do not have ibm_db installed on your system, this command will attempt to load it for you. If the %sql command does not work, it may be that this library is failing. To review the error messages, remove the %%capture clause.

# In[1]:


get_ipython().run_cell_magic(u'capture', u'', u'!pip install ibm_db\n!pip install --user --upgrade pixiedust')


# ### Install Db2 Extensions

# In[2]:


#
# Set up Jupyter MAGIC commands "sql". 
# %sql will return results from a DB2 select statement or execute a DB2 command
#
# IBM 2017: George Baklarz
# Version 2017-11-15
#

import ibm_db
import pandas
import ibm_db_dbi
import json
import matplotlib.pyplot as plt
import getpass
import os
import pickle
import time
import sys
import re
import warnings
warnings.filterwarnings("ignore")

# Override the name of display, HTML, and Image in the event you plan to use the pixiedust library for
# rendering graphics.

from IPython.display import HTML as pHTML, Image as pImage, display as pDisplay
from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
from pixiedust.display import *
from pixiedust.utils.shellAccess import ShellAccess

# Python Hack for Input between 2 and 3

try: 
    input = raw_input 
except NameError: 
    pass 

plt.style.use('ggplot')

settings = {
     "maxrows"  : 10,    
     "database" : "",
     "hostname" : "localhost",
     "port"     : "50000",
     "protocol" : "TCPIP",    
     "uid"      : "DB2INST1",
     "pwd"      : "password"
}

# Connection settings for statements 

connected = False
hdbc = None
hstmt = None
runtime = 1

def sqlhelp():
    
    sd = '<td style="text-align:left;">'
    ed = '</td>'
    sh = '<th style="text-align:left;">'
    eh = '</th>'
    sr = '<tr>'
    er = '</tr>'
    
    helpSQL = """
       <h3>SQL Options</h3> 
       <p>The following options are available as part of a SQL statement. The options are always preceded with a
       minus sign (i.e. -q).
       <table>
        {sr}
           {sh}Option{eh}
           {sh}Description{eh}
        {er}
        {sr}
          {sd}d{ed}
          {sd}Change SQL delimiter to "@" from ";"{ed}
        {er}
        {sr}
          {sd}q{ed}
          {sd}Quiet results - no answer set or messages returned from the function{ed}
        {er}
        {sr}
          {sd}n{ed}
          {sd}Execute all of the SQL as commands rather than select statements (no answer sets){ed}
        {er}
        {sr}
          {sd}s{ed}
          {sd}Execute everything as SELECT statements. By default, SELECT, VALUES, and WITH are considered part of an answer set, but it is possible that you have an SQL statement that does not start with any of these keywords but returns an answer set.
          {ed} 
        {er}
        {sr}  
          {sd}r{ed}
          {sd}Return the result set as an array of values{ed}
        {er}
        {sr}
          {sd}t{ed}
          {sd}Time the following SQL statement and return the number of times it executes in 1 second{ed}
        {er}
        {sr}
          {sd}j{ed}
          {sd}Create a pretty JSON representation. Only the first column is formatted{ed}
        {er}
        {sr}
          {sd}a{ed}
          {sd}Return all rows in answer set and do not limit display{ed}
        {er}
        {sr}
          {sd}i{ed}
          {sd}Return the data in a pixiedust display to view the data and optionally plot it.{ed}
        {er}
        {sr}
          {sd}pb{ed}
          {sd}Plot the results as a bar chart{ed}
        {er}
        {sr}
          {sd}pl{ed}
          {sd}Plot the results as a line chart{ed}
        {er}
        {sr}
          {sd}pp{ed}
          {sd}Plot Pie: Plot the results as a pie chart{ed}
        {er}
        {sr}
          {sd}sampledata{ed}
          {sd}Create and load the EMPLOYEE and DEPARTMENT tables{ed}
        {er}
       </table>
       """
    
    helpSQL = helpSQL.format(**locals())
        
    pDisplay(pHTML(helpSQL))
    
def connected_help():
    
    sd = '<td style="text-align:left;">'
    ed = '</td>'
    sh = '<th style="text-align:left;">'
    eh = '</th>'
    sr = '<tr>'
    er = '</tr>'
        
    helpConnect = """
       <h3>Connecting to DB2</h3> 
       <p>The CONNECT command has the following format:
       <pre>
       %sql CONNECT TO &lt;database&gt; USER &lt;userid&gt; USING &lt;password|?&gt; HOST &lt;ip address&gt; PORT &lt;port number&gt;
       </pre>
       If you use a "?" for the password field, the system will prompt you for a password. This avoids typing the 
       password as clear text on the screen. If a connection is not successful, the system will print the error
       message associated with the connect request.
       <p>
       Note: When prompted for input, you can use the format ip:port or #x:port where #x represents 
       the last digits of the containers IP address. For instance, if the Db2 server is found on 172.17.0.2 then
       you would use the value #2 to represent 172.17.0.2:50000. 
       <p>
       If the connection is successful, the parameters are saved on your system and will be used the next time you
       run an SQL statement, or when you issue the %sql CONNECT command with no parameters.
       <p>If you issue CONNECT RESET, all of the current values will be deleted and you will need to 
       issue a new CONNECT statement. 
       <p>CONNECT without any parameters will cause the program to prompt you for
       the values. To use the default values, just hit return for each input line. The default values are: 
       <table>
       {sr}
         {sh}Setting{eh}
         {sh}Description{eh}
       {er}
       {sr}
         {sd}DB2 Driver{ed}
         {sd}IBM DB2 ODBC Driver (this requires a manual change){ed}
       {er}
       {sr}
         {sd}Database{ed}{sd}SAMPLE{ed}
       {er}
       {sr}
         {sd}Hostname{ed}
         {sd}localhost (enter an IP address if you need to connect to a remote server){ed}
       {er}
       {sr}
         {sd}PORT{ed}
         {sd}50000 (this is the default but it could be different){ed}
       {er}
       {sr}         
         {sd}PROTOCOL{ed}
         {sd}TCPIP (You could have a local connection in Windows){ed}
       {er}
       {sr}         
         {sd}Userid{ed}
         {sd}DB2INST1{ed} 
       {er}
       {sr}                  
         {sd}Password{ed}
         {sd}No password is provided so you have to enter a value{ed}
       {er}
       {sr}       
         {sd}Maximum Rows{ed}
         {sd}10 lines of output are displayed when a result set is returned{ed}
       {er}
       </table>
       """
    
    helpConnect = helpConnect.format(**locals())
        
    pDisplay(pHTML(helpConnect))    
    
def split_ipport(in_port):

    # Split input into an IP address and Port number

    checkports = in_port.split(':')
    ip = checkports[0]
    if (len(checkports) > 1):
        port = checkports[1]
    else:
        port = None

    if (ip[:1] == '#'):
        ip = "172.17.0." + ip[1:]

    return ip, port    


def load_settings():

    # This routine will load the settings from the previous session if they exist
    
    global settings
    
    fname = "db2connect.pickle"

    try:
        with open(fname,'rb') as f: 
            settings = pickle.load(f) 
            
    except: 
        pass
    
    return

def save_settings():

    # This routine will save the current settings if they exist
    
    global settings
    
    fname = "db2connect.pickle"
    
    try:
        with open(fname,'wb') as f:
            pickle.dump(settings,f)
            
    except:
        errormsg("Failed trying to write DB2 Configuration Information.")
 
    return  

# DB2 Connection information Help

def db2_create_sample():
    
    create_department = """
      BEGIN
        DECLARE FOUND INTEGER; 
        SET FOUND = (SELECT COUNT(*) FROM SYSIBM.SYSTABLES WHERE NAME='DEPARTMENT' AND CREATOR=CURRENT USER); 
        IF FOUND = 0 THEN 
           EXECUTE IMMEDIATE('CREATE TABLE DEPARTMENT(DEPTNO CHAR(3) NOT NULL, DEPTNAME VARCHAR(36) NOT NULL, 
                              MGRNO CHAR(6),ADMRDEPT CHAR(3) NOT NULL)'); 
           EXECUTE IMMEDIATE('INSERT INTO DEPARTMENT VALUES 
             (''A00'',''SPIFFY COMPUTER SERVICE DIV.'',''000010'',''A00''), 
             (''B01'',''PLANNING'',''000020'',''A00''), 
             (''C01'',''INFORMATION CENTER'',''000030'',''A00''), 
             (''D01'',''DEVELOPMENT CENTER'',NULL,''A00''), 
             (''D11'',''MANUFACTURING SYSTEMS'',''000060'',''D01''), 
             (''D21'',''ADMINISTRATION SYSTEMS'',''000070'',''D01''), 
             (''E01'',''SUPPORT SERVICES'',''000050'',''A00''), 
             (''E11'',''OPERATIONS'',''000090'',''E01''), 
             (''E21'',''SOFTWARE SUPPORT'',''000100'',''E01''), 
             (''F22'',''BRANCH OFFICE F2'',NULL,''E01''), 
             (''G22'',''BRANCH OFFICE G2'',NULL,''E01''), 
             (''H22'',''BRANCH OFFICE H2'',NULL,''E01''), 
             (''I22'',''BRANCH OFFICE I2'',NULL,''E01''), 
             (''J22'',''BRANCH OFFICE J2'',NULL,''E01'')');      
           END IF;
      END"""
  
    get_ipython().magic(u'sql -d -q {create_department}')
    
    create_employee = """
     BEGIN
        DECLARE FOUND INTEGER; 
        SET FOUND = (SELECT COUNT(*) FROM SYSIBM.SYSTABLES WHERE NAME='EMPLOYEE' AND CREATOR=CURRENT USER); 
        IF FOUND = 0 THEN 
          EXECUTE IMMEDIATE('CREATE TABLE EMPLOYEE(
                             EMPNO CHAR(6) NOT NULL,
                             FIRSTNME VARCHAR(12) NOT NULL,
                             MIDINIT CHAR(1),
                             LASTNAME VARCHAR(15) NOT NULL,
                             WORKDEPT CHAR(3),
                             PHONENO CHAR(4),
                             HIREDATE DATE,
                             JOB CHAR(8),
                             EDLEVEL SMALLINT NOT NULL,
                             SEX CHAR(1),
                             BIRTHDATE DATE,
                             SALARY DECIMAL(9,2),
                             BONUS DECIMAL(9,2),
                             COMM DECIMAL(9,2)
                             )');
          EXECUTE IMMEDIATE('INSERT INTO EMPLOYEE VALUES
             (''000010'',''CHRISTINE'',''I'',''HAAS''      ,''A00'',''3978'',''1995-01-01'',''PRES    '',18,''F'',''1963-08-24'',152750.00,1000.00,4220.00),
             (''000020'',''MICHAEL''  ,''L'',''THOMPSON''  ,''B01'',''3476'',''2003-10-10'',''MANAGER '',18,''M'',''1978-02-02'',94250.00,800.00,3300.00),
             (''000030'',''SALLY''    ,''A'',''KWAN''      ,''C01'',''4738'',''2005-04-05'',''MANAGER '',20,''F'',''1971-05-11'',98250.00,800.00,3060.00),
             (''000050'',''JOHN''     ,''B'',''GEYER''     ,''E01'',''6789'',''1979-08-17'',''MANAGER '',16,''M'',''1955-09-15'',80175.00,800.00,3214.00),
             (''000060'',''IRVING''   ,''F'',''STERN''     ,''D11'',''6423'',''2003-09-14'',''MANAGER '',16,''M'',''1975-07-07'',72250.00,500.00,2580.00),
             (''000070'',''EVA''      ,''D'',''PULASKI''   ,''D21'',''7831'',''2005-09-30'',''MANAGER '',16,''F'',''2003-05-26'',96170.00,700.00,2893.00),
             (''000090'',''EILEEN''   ,''W'',''HENDERSON'' ,''E11'',''5498'',''2000-08-15'',''MANAGER '',16,''F'',''1971-05-15'',89750.00,600.00,2380.00),
             (''000100'',''THEODORE'' ,''Q'',''SPENSER''   ,''E21'',''0972'',''2000-06-19'',''MANAGER '',14,''M'',''1980-12-18'',86150.00,500.00,2092.00),
             (''000110'',''VINCENZO'' ,''G'',''LUCCHESSI'' ,''A00'',''3490'',''1988-05-16'',''SALESREP'',19,''M'',''1959-11-05'',66500.00,900.00,3720.00),
             (''000120'',''SEAN''     ,'' '',''O`CONNELL'' ,''A00'',''2167'',''1993-12-05'',''CLERK   '',14,''M'',''1972-10-18'',49250.00,600.00,2340.00),
             (''000130'',''DELORES''  ,''M'',''QUINTANA''  ,''C01'',''4578'',''2001-07-28'',''ANALYST '',16,''F'',''1955-09-15'',73800.00,500.00,1904.00),
             (''000140'',''HEATHER''  ,''A'',''NICHOLLS''  ,''C01'',''1793'',''2006-12-15'',''ANALYST '',18,''F'',''1976-01-19'',68420.00,600.00,2274.00),
             (''000150'',''BRUCE''    ,'' '',''ADAMSON''   ,''D11'',''4510'',''2002-02-12'',''DESIGNER'',16,''M'',''1977-05-17'',55280.00,500.00,2022.00),
             (''000160'',''ELIZABETH'',''R'',''PIANKA''    ,''D11'',''3782'',''2006-10-11'',''DESIGNER'',17,''F'',''1980-04-12'',62250.00,400.00,1780.00),
             (''000170'',''MASATOSHI'',''J'',''YOSHIMURA'' ,''D11'',''2890'',''1999-09-15'',''DESIGNER'',16,''M'',''1981-01-05'',44680.00,500.00,1974.00),
             (''000180'',''MARILYN''  ,''S'',''SCOUTTEN''  ,''D11'',''1682'',''2003-07-07'',''DESIGNER'',17,''F'',''1979-02-21'',51340.00,500.00,1707.00),
             (''000190'',''JAMES''    ,''H'',''WALKER''    ,''D11'',''2986'',''2004-07-26'',''DESIGNER'',16,''M'',''1982-06-25'',50450.00,400.00,1636.00),
             (''000200'',''DAVID''    ,'' '',''BROWN''     ,''D11'',''4501'',''2002-03-03'',''DESIGNER'',16,''M'',''1971-05-29'',57740.00,600.00,2217.00),
             (''000210'',''WILLIAM''  ,''T'',''JONES''     ,''D11'',''0942'',''1998-04-11'',''DESIGNER'',17,''M'',''2003-02-23'',68270.00,400.00,1462.00),
             (''000220'',''JENNIFER'' ,''K'',''LUTZ''      ,''D11'',''0672'',''1998-08-29'',''DESIGNER'',18,''F'',''1978-03-19'',49840.00,600.00,2387.00),
             (''000230'',''JAMES''    ,''J'',''JEFFERSON'' ,''D21'',''2094'',''1996-11-21'',''CLERK   '',14,''M'',''1980-05-30'',42180.00,400.00,1774.00),
             (''000240'',''SALVATORE'',''M'',''MARINO''    ,''D21'',''3780'',''2004-12-05'',''CLERK   '',17,''M'',''2002-03-31'',48760.00,600.00,2301.00),
             (''000250'',''DANIEL''   ,''S'',''SMITH''     ,''D21'',''0961'',''1999-10-30'',''CLERK   '',15,''M'',''1969-11-12'',49180.00,400.00,1534.00),
             (''000260'',''SYBIL''    ,''P'',''JOHNSON''   ,''D21'',''8953'',''2005-09-11'',''CLERK   '',16,''F'',''1976-10-05'',47250.00,300.00,1380.00),
             (''000270'',''MARIA''    ,''L'',''PEREZ''     ,''D21'',''9001'',''2006-09-30'',''CLERK   '',15,''F'',''2003-05-26'',37380.00,500.00,2190.00),
             (''000280'',''ETHEL''    ,''R'',''SCHNEIDER'' ,''E11'',''8997'',''1997-03-24'',''OPERATOR'',17,''F'',''1976-03-28'',36250.00,500.00,2100.00),
             (''000290'',''JOHN''     ,''R'',''PARKER''    ,''E11'',''4502'',''2006-05-30'',''OPERATOR'',12,''M'',''1985-07-09'',35340.00,300.00,1227.00),
             (''000300'',''PHILIP''   ,''X'',''SMITH''     ,''E11'',''2095'',''2002-06-19'',''OPERATOR'',14,''M'',''1976-10-27'',37750.00,400.00,1420.00),
             (''000310'',''MAUDE''    ,''F'',''SETRIGHT''  ,''E11'',''3332'',''1994-09-12'',''OPERATOR'',12,''F'',''1961-04-21'',35900.00,300.00,1272.00),
             (''000320'',''RAMLAL''   ,''V'',''MEHTA''     ,''E21'',''9990'',''1995-07-07'',''FIELDREP'',16,''M'',''1962-08-11'',39950.00,400.00,1596.00),
             (''000330'',''WING''     ,'' '',''LEE''       ,''E21'',''2103'',''2006-02-23'',''FIELDREP'',14,''M'',''1971-07-18'',45370.00,500.00,2030.00),
             (''000340'',''JASON''    ,''R'',''GOUNOT''    ,''E21'',''5698'',''1977-05-05'',''FIELDREP'',16,''M'',''1956-05-17'',43840.00,500.00,1907.00),
             (''200010'',''DIAN''     ,''J'',''HEMMINGER'' ,''A00'',''3978'',''1995-01-01'',''SALESREP'',18,''F'',''1973-08-14'',46500.00,1000.00,4220.00),
             (''200120'',''GREG''     ,'' '',''ORLANDO''   ,''A00'',''2167'',''2002-05-05'',''CLERK   '',14,''M'',''1972-10-18'',39250.00,600.00,2340.00),
             (''200140'',''KIM''      ,''N'',''NATZ''      ,''C01'',''1793'',''2006-12-15'',''ANALYST '',18,''F'',''1976-01-19'',68420.00,600.00,2274.00),
             (''200170'',''KIYOSHI''  ,'' '',''YAMAMOTO''  ,''D11'',''2890'',''2005-09-15'',''DESIGNER'',16,''M'',''1981-01-05'',64680.00,500.00,1974.00),
             (''200220'',''REBA''     ,''K'',''JOHN''      ,''D11'',''0672'',''2005-08-29'',''DESIGNER'',18,''F'',''1978-03-19'',69840.00,600.00,2387.00),
             (''200240'',''ROBERT''   ,''M'',''MONTEVERDE'',''D21'',''3780'',''2004-12-05'',''CLERK   '',17,''M'',''1984-03-31'',37760.00,600.00,2301.00),
             (''200280'',''EILEEN''   ,''R'',''SCHWARTZ''  ,''E11'',''8997'',''1997-03-24'',''OPERATOR'',17,''F'',''1966-03-28'',46250.00,500.00,2100.00),
             (''200310'',''MICHELLE'' ,''F'',''SPRINGER''  ,''E11'',''3332'',''1994-09-12'',''OPERATOR'',12,''F'',''1961-04-21'',35900.00,300.00,1272.00),
             (''200330'',''HELENA''   ,'' '',''WONG''      ,''E21'',''2103'',''2006-02-23'',''FIELDREP'',14,''F'',''1971-07-18'',35370.00,500.00,2030.00),
             (''200340'',''ROY''      ,''R'',''ALONZO''    ,''E21'',''5698'',''1997-07-05'',''FIELDREP'',16,''M'',''1956-05-17'',31840.00,500.00,1907.00)');                             
        END IF;
     END"""
    
    get_ipython().magic(u'sql -d -q {create_employee}')
    
    success("Sample tables [EMPLOYEE, DEPARTMENT] created.")
    
def connected_prompt():
    
    global settings

    
    settings["database"] = input("Enter the database name [SAMPLE]: ") or "SAMPLE";
    hostport = input("Enter the HOST IP address and PORT in the form ip:port or #x:port [localhost:50000].") or "localhost:50000";
    ip, port = split_ipport(hostport)
    if (port == None): port = "50000"
    settings["hostname"] = ip
    settings["port"]     = port
    settings["uid"]      = input("Enter Userid on the DB2 system [DB2INST1]: ").upper() or "DB2INST1";
    settings["pwd"]      = getpass.getpass("Password [password]: ") or "password";
    settings["maxrows"]  = input("Maximum rows displayed [10]: ") or "10";
    settings["maxrows"]  = int(settings["maxrows"])

# Connect to DB2 and prompt if you haven't set any of the values yet

def db2_doConnect():
    
    global hdbc, hstmt, connected, runtime
    global settings  

    if connected == False: 
        
        if len(settings["database"]) == 0:
            connected_help()
            connected_prompt()
    
    dsn = (
           "DRIVER={{IBM DB2 ODBC DRIVER}};"
           "DATABASE={0};"
           "HOSTNAME={1};"
           "PORT={2};"
           "PROTOCOL=TCPIP;"
           "UID={3};"
           "PWD={4};").format(settings["database"], settings["hostname"], settings["port"], settings["uid"], settings["pwd"])

    # Get a database handle (hdbc) and a statement handle (hstmt) for subsequent access to DB2

    try:
        hdbc  = ibm_db.connect(dsn, "", "")
    except Exception as err:
        errormsg(str(err))
        connected = False
        settings["database"] = ''
        return
    
    try:
        hstmt = ibm_db_dbi.Connection(hdbc)
    except Exception as err:
        errormsg(str(err))
        connected = False
        settings["database"] = ''
        return        
    
    connected = True
    
    # Save the values for future use
    
    save_settings()
    
    success("Connection successful.")
    
# Parse the CONNECT statement and execute if possible 

def parseConnect(inSQL):
    
    global settings, connected

    connected = False
    
    cParms = inSQL.split()
    cnt = 0
    
    while cnt < len(cParms):
        if cParms[cnt].upper() == 'TO':
            if cnt+1 < len(cParms):
                settings["database"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No database specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'USER':
            if cnt+1 < len(cParms):
                settings["uid"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No userid specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'USING':
            if cnt+1 < len(cParms):
                settings["pwd"] = cParms[cnt+1]   
                if (settings["pwd"] == '?'):
                    settings["pwd"] = getpass.getpass("Password [password]: ") or "password"
                cnt = cnt + 1
            else:
                errormsg("No password specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'HOST':
            if cnt+1 < len(cParms):
                settings["hostname"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No hostname specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'PORT':                           
            if cnt+1 < len(cParms):
                settings["port"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No port specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'RESET': 
             settings["database"] = ''
             success("Connection reset.")
             return
        else:
            cnt = cnt + 1
                     
    db2_doConnect()

# Find a keyword in a SQL string

def findKeyword(keywords,keyword):
    
    if len(keywords) == 0: return False
    if len(keyword) == 0: return False
    
    uKeywords = keywords.upper()
    uKeyword  = keyword.upper()
    
    if uKeywords.find(uKeyword.strip()) >= 0:
        return True
    else:
        return False
    
# Run a command for one second to see how many times we execute it and return the count

def sqlTimer(flag_cmd, inSQL):
    
    global hdbc, hstmt, runtime

    db2Block = 2
    count = 0
    t_end = time.time() + runtime
    while time.time() < t_end:
        if (flag_cmd == db2Block):
            try:
                stmt = ibm_db.exec_immediate(hdbc,inSQL)
            except Exception as err:
                db2_error(False)
                return(-1)
        else:
            try:
                stmt = ibm_db.exec_immediate(hdbc,inSQL)
                while( ibm_db.fetch_row(stmt) ): pass
            except Exception as err:
                db2_error(False)
                return(-1)
        
        count = count + 1
        
    return(count)

# Print out the DB2 error generated by the last executed statement

def db2_error(quiet):
    
    if quiet == True: return

    html = '<p style="border:2px; border-style:solid; border-color:#FF0000; background-color:#ffe6e6; padding: 1em;">'

    errmsg = ibm_db.stmt_errormsg().replace('\r',' ')
    errmsg = errmsg[errmsg.rfind("]")+1:].strip()
    pDisplay(pHTML(html+errmsg+"</p>"))
    
# Print out an error message

def errormsg(message):
    
    html = '<p style="border:2px; border-style:solid; border-color:#FF0000; background-color:#ffe6e6; padding: 1em;">'
    pDisplay(pHTML(html + message + "</p>"))    
    
def success(message):
    
    print(message)
#
#    Optional Code to print green border around success statements
#
#    html = '<p style="border:2px; border-style:solid; border-color:#008000; background-color:#e6ffe6; padding: 1em;">'
#            
#    if message != "":
#        pDisplay(pHTML(html + message + "</p>"))
        
    return     
        
@magics_class
class DB2(Magics):
      
    import pixiedust  
    
    @line_cell_magic
    def sql(self, line, cell=None):
            
        # Before we event get started, check to see if you have connected yet. Without a connection we 
        # can't do anything. You may have a connection request in the code, so if that is true, we run those,
        # otherwise we connect immediately
        
        # If your statement is not a connect, and you haven't connected, we need to do it for you
    
        global settings 
        global hdbc, hstmt, connected
        
        select = ["SELECT", "WITH", "VALUES"] 
        noBlock = 0
        sqlBlock = 1
        db2Block = 2
            
        # If you use %sql (line) we just run the SQL. If you use %%SQL the entire cell is run.
        
        flag_delim = ";"
        flag_results = True
        flag_sqlType = noBlock
        flag_quiet = False
        flag_json = False
        flag_timer = False
        flag_plot = 0
        flag_cell = False
        flag_output = False
        flag_resultset = False
        flag_dataframe = False
        
        # The parameters must be in the line, not in the cell i.e. %sql -c 
        
        Parms = line.strip()
        
        if len(Parms) == 0:
            if cell == None: 
                sqlhelp()
                return
            if len(cell.strip()) == 0: 
                sqlhelp()
                return
            
        # Check of you just want help
        
        if Parms == "?":
            sqlhelp()
            return
        
        if Parms.upper() == "? CONNECT":
            connected_help()
            return
        
        # If you issue a CONNECT statement in %sql then we run this first before auto-connecting
        if findKeyword(Parms,"CONNECT") == True: 
            parseConnect(Parms)
            return
        
        # We need to check to see if we are connected before running any SQL
        if connected == False:
            db2_doConnect()
            if connected == False: return
            
        # Default result set size
        if settings["maxrows"] == -1:
            pandas.reset_option('max_rows')
        else:
            pandas.options.display.max_rows = settings["maxrows"]
      
        # Display rows as JSON structure
        if Parms.find("-j") >= 0:
            flag_json = True
            Parms = Parms.replace("-j"," ")
          
        # Load sample tables for scripts
        if Parms.find('-sampledata') >= 0:
            db2_create_sample()
            return
        
        # Execute the SQL so that it behaves like a SELECT statement
        if Parms.find("-s") >= 0:
            flag_sqlType = sqlBlock
            Parms = Parms.replace("-s"," ")
            
        # Execute the SQL but return the results in an array (basically a two-dimensional array)
        if Parms.find("-r") >= 0:
            flag_resultset = True
            Parms = Parms.replace("-r", " ")
                     
        # Execute the SQL so that it behaves like an INSERT, DELETE, UPDATE or no result set
        if Parms.find("-n") >= 0:
            flag_sqlType = db2Block
            Parms = Parms.replace("-n"," ")
            
        # Quiet execution (no errors or completed messages)
        if Parms.find("-q") >= 0:
            flag_quiet = True
            Parms = Parms.replace("-q"," ")

        # Retrieve all rows (do not use the default limit)
        if Parms.find("-a") >= 0:
            pandas.reset_option('max_rows')
            Parms = Parms.replace("-a"," ")
          
        # Set the delimiter to @ instead of a semi-colon for procedures, triggers, and functions
        if Parms.find("-d") >= 0:
            flag_delim = "@"
            Parms = Parms.replace("-d"," ")        
            
        # Timer function (not that useful, but worth a try)
        if Parms.find("-t") >= 0:
            flag_timer = True
            Parms = Parms.replace("-t"," ")
          
        # Plot functions -pb = bar, -pp = pie, -pl = line
        if Parms.find("-pb") >= 0:
            flag_plot = 1
            Parms = Parms.replace("-pb"," ")
          
        if Parms.find("-pp") >= 0:
            flag_plot = 2
            Parms = Parms.replace("-pp"," ")
                                
        if Parms.find("-pl") >= 0:
            flag_plot = 3
            Parms = Parms.replace("-pl"," ") 
            
        if Parms.find("-i") >= 0:
            flag_plot = 4
            Parms = Parms.replace("-i"," ")             
      
        remainder = Parms.strip()
                    
        # Split the line according to your delimiter
            
        if cell is None:
            sqlLines = [remainder]
            flag_cell = False
        else:
            cell = re.sub('.*?--.*$',"",cell,flags=re.M)
            remainder = cell.replace("\n"," ")
            sqlLines = remainder.split(flag_delim)
            flag_cell = True
                      
        # For each line figure out if you run it as a command (db2) or select (sql)
         
        for sql in sqlLines:

            # Split the line so we know what the first keyword is. We only look at the first one. There may
            # be SQL that returns output that we may not know about
            
            keywords = sql.split()
            if len(keywords) == 0: continue
            
            sqlcmd = keywords[0].upper()
            
            if (flag_timer == True):
                    
                count = sqlTimer(flag_sqlType, sql)
                 
                if flag_quiet == False and count != -1:
                    print("Total iterations in %s second(s): %s" % (runtime,count))
                    
                return(count)
            
            elif (flag_plot != 0):
                
                try:
                    df = pandas.read_sql(sql,hstmt)
                except Exception as err:
                    db2_error(False)
                    return
                
                if flag_plot == 4:
                    
                    ShellAccess.pdf = df
                    display(pdf)

                    return
                
                plt.style.use('ggplot')
                plt.figure()
                col_count = len(df.columns)
                
                if flag_plot == 1:

                    # Bar Chart
                    if (col_count >= 2):
                        xlabel = df.columns.values[0]
                        ylabel = df.columns.values[1]
                        _ = df.plot(kind='bar',x=xlabel,y=ylabel);
                    else:
                        _ = df.plot(kind='bar');
                        
                    
                elif flag_plot == 2:
                    
                    # Pie 
                    if (col_count >= 2):
                        xlabel = df.columns.values[0]
                        xname = df[xlabel].tolist()
                        yname = df.columns.values[1]
                        _ = df.plot(kind='pie',y=yname,labels=xname);
                    else:
                        yname = df.columns.values[0]
                        _ = df.plot(kind='pie',y=yname);
                    
                elif flag_plot == 3:
                    
                    # Line Chart
                    if (col_count >= 2):                    
                        xlabel = df.columns.values[0]
                        ylabel = df.columns.values[1]
                        _ = df.plot(kind='line',x=xlabel,y=ylabel) ;                   
                    else:
                        _ = df.plot(kind='line') ;                      
                        
                else:
                    return
                
                plt.show()
                return
 
            elif (flag_sqlType == sqlBlock) or (sqlcmd in select and flag_sqlType != db2Block):
                
                if flag_json == True:
                    try: 
                        stmt = ibm_db.exec_immediate(hdbc,sql);
                        row_count = 0
                        while( ibm_db.fetch_row(stmt) ):
                            row_count = row_count + 1
                            jsonVal = ibm_db.result(stmt,0)
                            formatted_JSON = json.dumps(json.loads(jsonVal), indent=4, separators=(',', ': '))
                        
                            # Print JSON Structure
                        
                            if row_count > 1: print()
                            print("Row: %d" % row_count)
                            print(formatted_JSON)
                            flag_output = True
                
                    except Exception as err:
                        db2_error(flag_quiet)
                    
                else:  
                    if flag_resultset == True:
                        row_count = 0
                        resultSet = []
                        try:
                            stmt = ibm_db.exec_immediate(hdbc,sql)
                            result = ibm_db.fetch_tuple(stmt)
                            while (result):
                                row = []
                                for col in result:
                                    row.append(col)
                            
                                resultSet.append(row)
                                result = ibm_db.fetch_tuple(stmt)
                            
                            return(resultSet)                                    
                                
                        except Exception as err:
                                db2_error(False) 
                        
                    else:
                        try:
                        
                            dp = pandas.read_sql(sql, hstmt)
                            if flag_dataframe == True:
                                return(dp)
                            else:
                                # pDisplay(dp)
                                flag_output = True
                                return(dp)
                
                        except Exception as err:
                            db2_error(flag_quiet)
                
            else:
                
                try: 
                    ibm_db.exec_immediate(hdbc,sql);
                    if flag_cell == False and flag_quiet == False:
                        print("Command completed.")
                
                except Exception as err:
                    db2_error(flag_quiet)
                    
        if flag_cell == True and flag_output == False:
            print("Command completed.")
            
# Register the Magic extension in Jupyter    
ip = get_ipython()          
ip.register_magics(DB2)
load_settings()
success("DB2 Extensions Loaded.")


# Set the table formatting to left align a table in a cell. By default, tables are centered in a cell. Remove this cell if you don't want to change Jupyter notebook formatting for tables.

# In[3]:


get_ipython().run_cell_magic(u'html', u'', u'<style>\n  table {margin-left: 0 !important; text-align: left;}\n</style>')


# #### Credits: IBM 2017, George Baklarz [baklarz@ca.ibm.com]
