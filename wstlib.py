#!/usr/bin/python
#
#	WSTLIB contains generic functions for the weatherstation program
#	Centralized in this Module
#	Author:	V Ankenbrand
#	Date:	17/12/2016
#

import sys, os, getopt, string, mysql.connector, wst_l1, wst_l2

#------------------------------------------------------------------------------
#
#	Global variables and Storage section for the library
#
#------------------------------------------------------------------------------
#	Time stamp data
#------------------------------------------------------------------------------
tim=""
tstmp=""

#------------------------------------------------------------------------------
#	Global work variables and buffers
#------------------------------------------------------------------------------
datstrg=[]                                           # record buffer for database
 
#------------------------------------------------------------------------------
#
#	Class Section
#
#------------------------------------------------------------------------------
#
#       CLASS vafile: 	opens weather station datafile, reads data into buffer
#			processes buffer records, finds min and max values
#			creates list in global memory to pass on to vadb class
#
#------------------------------------------------------------------------------


class vafile:
   "generic class for file processing"

   tim
   fname = ""
   buf=""
   rtctr = 0
   rc = 0
   
#------------------------------------------------------------------------------
#	Class constructor: open file and read into buffer
#------------------------------------------------------------------------------

   def __init__(self,filname):
      vafile.fname=filname
      vafile.tim=wst_l2.getTime()
      vafile.rtctr += 1
      print "Class vafile loaded ..."
      print "Filename: ",vafile.fname
      print "Class time: ",vafile.tim

      try:
         with open(filname) as f:                     # read weather data 
            vafile.buf=f.read().splitlines()
      except IOError:
         print "Can't open weather data file"
         vafile.rc = 8 

      f.close() 

#------------------------------------------------------------------------------
#	Class Methods
#------------------------------------------------------------------------------
#       Countlines: return number of lines in buffer
#------------------------------------------------------------------------------

   def countLines(self):
      return len(vafile.buf)

#------------------------------------------------------------------------------
#       procWstdata: 	process the wstation file:
#			group processing by date
#			find minimal / maximal values
#			map data to the global datstrg string for processing
#			in the DB class 
#------------------------------------------------------------------------------

   def procWstdata(self):

#------------------------------------------------------------------------------     
#	Initialize group change fields 
#	Process all file line items
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#	Workdata
#------------------------------------------------------------------------------
      global maxtmp, mintmp, maxhum, minhum, maxpres, minpres, maxwind, maxrain
      global wtmp,whum,wpres,wwind,wrain
      global datstrg,errctr

      fdat=0
      wdat=0
      lctr=0
      errctr=0
      lstr=""

      wst_l1.initGroup()
      
#      print "procWstdata - errctr = ", errctr

      for lin in vafile.buf:
        fdat=wst_l1.convDate(lin)
        lctr+=1
        
#        print "Fdat: %s\t Wdat: %s" % (fdat,wdat)

        if (fdat != wdat) and (wdat > 0):	          	# Date change
           lstr=wst_l1.createStrg(wdat)                 # build string for in memory data rec list
           datstrg.append(lstr)
#           print lstr
           wst_l1.initGroup()

        wdat = fdat
        wst_l1.mapFields(lin,lctr)
          
#        print "procWstdata loop - errctr = ", errctr  
#
#	Processing of last record in buffer (flush the buffer)
#
      lstr=wst_l1.createStrg(wdat)
      datstrg.append(lstr)
            
      print "%d Records read from file: %s" % (len(vafile.buf),vafile.fname)
      print "%d daily records generated" % len(datstrg)
      print "%d data errors found" % wst_l1.errctr


#------------------------------------------------------------------------------
#
#	CLASS vadb:	inserts records into wstation db
#			records are stored in data list in global memory
#
#------------------------------------------------------------------------------

class vadb:
   "generic class for maria db processing"

#------------------------------------------------------------------------------
#    Data section
#------------------------------------------------------------------------------
   tim, tstmp;
   dbnam = ""
   tabnam="wdata"
   dbh = ""
   buf=""
   recno = 0
   sdate = ""
   edate = ""
   rc = 0
   rtctr=0

#------------------------------------------------------------------------------
#       Class constructor: open database 
#------------------------------------------------------------------------------

   def __init__(self,dbnam):
      vadb.tim=wst_l2.getTime()
      vadb.dbnam=dbnam
      vadb.rtctr += 1
      tabinf=""
      tabnam="wdata"
      
      print "Class vadb loaded ..."
#      print "Database: ",vadb.dbnam
      print "Class time: ",vadb.tim

      try:
         vadb.dbh=mysql.connector.connect(user="volker",database=dbnam)
      except IOError:
         print "Can't open the weather database"
         vadb.rc = 8   

      print "Database: %s opened " % vadb.dbnam
#
#    Get weather station table info and load into class variables
#
      tabinf=getTabinfo(vadb.dbh,tabnam)
      
      vadb.recno=int(tabinf.split(",")[0])
      vadb.sdate=tabinf.split(",")[1]
      vadb.edate=tabinf.split(",")[2]   
      
#------------------------------------------------------------------------------
#       Class Methods
#------------------------------------------------------------------------------
#       countTable returns number of data in table wdata
#------------------------------------------------------------------------------

#   def countTable(self):
#      q="SELECT COUNT(*) FROM wdata"
#      cur=vadb.dbh.cursor()
#      cur.execute(q)
      
#      for ret in cur:
#          rctr=ret[0]
#      
#      return rctr

#------------------------------------------------------------------------------
#       loadData:	process the wstation file:
#                       group processing by date
#                       find minimal / maximal values
#                       map data to the global datstrg string for processing
#                       in the DB class
#------------------------------------------------------------------------------

   def loadData(self):
      global datstrg
      tabnam="wdata" 
      tabinf=""
      ctro=vadb.recno
       
#
#	Create cursor on DB wstation
#

      cur=vadb.dbh.cursor()

      for litem in datstrg:
        q=wst_l1.createQuery(litem)
        cur.execute(q)
        cur.execute("COMMIT")

            
      tabinf=getTabinfo(vadb.dbh,tabnam)
      
      vadb.recno=int(tabinf.split(",")[0])
      vadb.sdate=tabinf.split(",")[1]
      vadb.edate=tabinf.split(",")[2]
#      vadb.recno=countTable(vadb.dbh,vadb.tabnam)
      ctri=vadb.recno-ctro
      print "%d records inserted into table wdata" % vadb.recno 
      vadb.dbh.close()
      print "Database closed"

      
#------------------------------------------------------------------------------
#       delTab:         delete all records from wdata table
#------------------------------------------------------------------------------

   def delTab(self):

      tabinf="" 
      tabnam="wdata"             
#      ctro=(self).countTable()
      ctro=vadb.recno
      print "Records in table wdata: ",ctro

#
#    Create cursor on DB wstation
#
      cur=vadb.dbh.cursor()
      cur.execute("DELETE FROM wdata")
      cur.execute("COMMIT")
      
      tabinf=getTabinfo(vadb.dbh,tabnam)
      
      vadb.recno=int(tabinf.split(",")[0])
      vadb.sdate=tabinf.split(",")[1]
      vadb.edate=tabinf.split(",")[2]
      
#      ctra=(self).countTable()
      print "Records in table wdata after deletion: ", vadb.recno
      
      ctr=ctro-vadb.recno
      print "%d records deleted" % ctr
      
      vadb.dbh.close()
      print "Database closed"
      
            
#------------------------------------------------------------------------------
#       dispTab:         display selected records from wdata table by from - to - date
#------------------------------------------------------------------------------

   def dispTab(self,sdat,edat):

      tabnam="wdata"
      head1='{:10s} {:12s} {:12s} {:16s} {:6s} {:6s}'.format("Date",'Temperature','Humidity','Pressure','Wind','Rain')
      head2='{:12s} {:5s} {:6s} {:6s} {:7s} {:7s} {:8s}'.format("",'Max','Min','Max','Min','Max','Min')
      
      q1="SELECT * FROM "+tabnam+" WHERE wdate = "+str(sdat)
      q2="SELECT * FROM "+tabnam+" WHERE wdate >= "+str(sdat)+" AND wdate <= "+str(edat)
      
#
#    Create cursor on DB wstation
#
      cur=vadb.dbh.cursor()
   
#
#    Check for single date or date range and selection of the according query
#     
      if (edat == 0):
         cur.execute(q1)
      else:
         cur.execute(q2)
         
      print head1
      print head2
      
      for wrec in cur:
          pstr=wst_l1.createPrnStrg(wrec)
          print pstr
                
      vadb.dbh.close()
      print "Database closed and leaving dispTab"
                     
#------------------------------------------------------------------------------
#
#	END OF CLASS SECTION
#
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#
#    Library routine SECTION
#
#------------------------------------------------------------------------------
#
#       Check and process line parameters
#
def getLinpar(argv):
  "Check line parameter: valid -c -d -h -i -p -u"
#------------------------------------------------------------------------------
#  print "Length argv: ", len(argv)
#
#    No line parameter
#

  emess="""Correct syntax
              - check data:     wst.py -c <file name>
              - show DB info:   wst.py -i
              - display table   wst.py -d <date(YYYYMMDD)> or <date range (YYYYMMDD-YYYYMMDD)] 
              - data upload:    wst.py -u <file name>
              - purge data:     wst.py -p
              - help:           wst.py -h"""

  if len(argv) < 1:
     print "Missing parameter !\n",emess
     sys.exit(3)

#
#    options: -h, -u: u expects the file name as an argument
#
  try:
    opts,args=getopt.getopt(argv,"c:d:hipu:")
  except getopt.GetoptError:
    print "Wrong command !"
    print emess
    sys.exit(4)
  
  for opt,arg in opts:
#    print "Opt: ",opt
#    print "Arg: ",arg
    if opt=="-h":
      print emess
      sys.exit()
    elif opt=="-c":
      filname=arg
    elif opt=="-d":
      dbdat=arg    
#      print "Filename: ",filname
      val=opt+" "+dbdat    
    elif opt=="-u":
      filname=arg
      val=opt+" "+filname
    elif ((opt=="-p") or (opt=="-i")):
      val=opt+" void"
#   print "Value of lineparameter: ", val
    return val


#
#       setTime sets tim and tstmp as global variables in wstlib
#
def setTime():

   global tim, tstmp

   tim=wst_l2.getTime()

   tstmp=wst_l2.getTstmp()
#
#       END of setTime()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#       getTabinfo returns record number and if not empty, date of 1st and last rec
#------------------------------------------------------------------------------

def getTabinfo(dbh,tabnam):
#------------------------------------------------------------------------------
#    Data section
#------------------------------------------------------------------------------
   c="," 
   sdat=""
   edat=""
   rval="" 
   q="SELECT COUNT(*) FROM "+tabnam
   qs1="SELECT wdate from " + tabnam + " order by wdate asc limit 1"
   qs2="SELECT wdate from " + tabnam + " order by wdate desc limit 1"
   
#------------------------------------------------------------------------------
#    Submit queries to DB: 1. No of records, 2. date of 1st record, 3. date of last rec
#------------------------------------------------------------------------------
   cur=dbh.cursor()
   cur.execute(q)
      
   for ret in cur:
       rctr=ret[0]
   
   if (rctr>0):
     cur.execute(qs1)
     
     for ret in cur:
       sdate=str(ret[0])
       
     cur.execute(qs2)
     
     for ret in cur:
       edate=str(ret[0])  
   
   rval=str(rctr)+c+sdat+c+edat   
   return rval
#------------------------------------------------------------------------------
#    END of getTabinfo(dbh,tabname)
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
#    END OF Library routine SECTION
#------------------------------------------------------------------------------
#=====================  End of Script ==========================================
