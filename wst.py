#!/usr/bin/python
#
#	Program for Jaycar Weatherstation Control
#	Basic functionality:
#		Read text file and insert data into MariaDb
#		Read database for specific date or from - to range
#
#	Principle: create classes in wstlib.py that can be used at a later stage
#		in a generic va.lib (e.g. DB module, File module etc)
#	Author:	V Ankenbrand
#	Date:	17/12/2016
#
#
#	Modules needed: subprocess for pipe operation on sapcontrol
#			string for added string functionality
#
 
import sys, string, wstlib

#------------------------------------------------------------------------------
#    Data Section
#------------------------------------------------------------------------------
fil=""                                              # File name   
wdat=""                                             # Date parameter: single or range   
dlst=[]                                             # Date list e.g. from date - end date

#------------------------------------------------------------------------------
#    Subroutine Section
#------------------------------------------------------------------------------

#==============================================================================
#    Data check routine: read data from file and check the numeric fields 
#==============================================================================
def chkData(fil):

#
#    create class instance vafile
#
  print "Function: check file data" 
  
  ifil=wstlib.vafile(fil)
  ifil.procWstdata()
  del ifil
  print "Class vafile unloaded..."
  wstlib.setTime()
  print "Program ended at ",wstlib.tim

#==============================================================================
#    Data load routine: read data from file and load to wdat table 
#==============================================================================
def loadData(fil):

#
#    create class instance vafile
#
  print "Function: file upload to db" 
  
  ifil=wstlib.vafile(fil)
  ifil.procWstdata()
  del ifil
  print "Class vafile unloaded..."

#
#    create instance vadb
#
  idb=wstlib.vadb("wstation")
  idb.loadData()

  wstlib.setTime()
  print "Program ended at ",wstlib.tim

#==============================================================================
#    Delete records from wdat table in database wstation
#==============================================================================
def delTable():
#
#    create instance vadb
#
  print "Function: purge db records" 

  idb=wstlib.vadb("wstation")
  idb.delTab()

  wstlib.setTime()
  print "Program ended at ",wstlib.tim


#==============================================================================
#    Display database records from wstation table
#==============================================================================
def dispDta(wdat):

  sdat=0
  edat=0
      
#
#    create instance vadb
#
  print "Function: display db records" 
    
  dlst=wdat.split("-")
  sdat=dlst[0]
  
  if (len(dlst)==2):
    edat=dlst[1] 
    
  print "Startdate: %s \tEnd date: %s" % (sdat,edat)

  idb=wstlib.vadb("wstation")
  idb.dispTab(sdat,edat)

  wstlib.setTime()
  print "Program ended at ",wstlib.tim


#==============================================================================
#    Print table info: number of records, date of first and last record
#==============================================================================
def printTabinf():
#
#    create instance vadb
#
  tabinf=""
  tabnam="wdata"
  
  print "Function: table info" 
  
  idb=wstlib.vadb("wstation")
  
  print "Table wdata contains %d records" % idb.recno
  if (idb.recno > 0):
    print "Start date: %s\tEnd date: %s" % (idb.sdate,idb.edate)

  wstlib.setTime()
  print "Program ended at ",wstlib.tim
#------------------------------------------------------------------------------
#    MAIN Routine: program entry point
#------------------------------------------------------------------------------
#
#	Main routine as wrapper to wstlib.py
#
#        Check line parameters first
#        in case of command line syntax errors subroutine will exit
#        Current available options: 
#            display: -h = help, -c = check data in file for errors -d display DB recs -i DB info
#            change data:    -u: upload data from file into db    -p purge (all) records from db
#

if __name__=="__main__":
    linpar=wstlib.getLinpar(sys.argv[1:])

wstlib.setTime()

print "Program started:\t", wstlib.tim
print "Lineparameter: %s" % linpar

mswtch=linpar.split()[0].upper()

if (mswtch == "-P"):
   delTable()
elif (mswtch == "-C"):
   fil=linpar.split()[1] 
   chkData(fil) 
elif (mswtch == "-D"):
   wdat=linpar.split()[1]
   dispDta(wdat)       
elif (mswtch == "-I"):
   printTabinf()    
elif (mswtch == "-U"):
   fil=linpar.split()[1]
   loadData(fil)

#==============================================================================
#    END OF MAIN ROUTINE
#==============================================================================