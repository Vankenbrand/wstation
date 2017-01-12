#!/usr/bin/python
#
#	WSTLIB contains generic functions for the weatherstation program
#	Centralized in this Module
#	Author:	V Ankenbrand
#	Date:	17/12/2016
#
 
import string 

#------------------------------------------------------------------------------
#
#    Global variables and Storage section for module wst_l1
#
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
#    Weather data: Min and Max values
#------------------------------------------------------------------------------
mintmp=0
maxtmp=0
minhum=0
maxhum=0
minpres=0
maxpres=0
maxwind=0
maxrain=0
wtmp=0
whum=0
wpres=0
wwind=0
wrain=0

#------------------------------------------------------------------------------
#    Global work variables and buffers
#------------------------------------------------------------------------------
errctr=0

#------------------------------------------------------------------------------
#
#       Level 1 Functions
#
#------------------------------------------------------------------------------
#
#       Initialize group fields
#

def initGroup():

   global maxtmp, mintmp, maxhum, minhum, maxpres, minpres, maxwind, maxrain

   maxtmp=float(-100)
   mintmp=float(100)
   maxhum=float(-1)
   minhum=float(120)
   maxpres=float(-1)
   minpres=float(2000)
   maxwind=float(-1)
   maxrain=float(-1)
#
#	End of initGroup()
#------------------------------------------------------------------------------


#
#       Split the date field from the file that comes as "dd/mm/yyyy hh:mm"
#	Return it in the format: "yyyymmdd"
#

def convDate(lin):

   l=lin.split()

   ldat=l[1].split("/")[2].split()[0]+l[1].split("/")[1]+l[1].split("/")[0].zfill(2)
   return ldat
#
#	End of convDate()
#------------------------------------------------------------------------------

#
#    routine mapFields is used by option -c (check data consistency) and
#        function -u: upload data file to database
#
#       Map file field structure to DB structure
#        Check data quality: temperature, humidity, pressure, wind speed and rain
#        are numeric fields: temperature could be potentially minus hence the lstrip('-') 
#        before the numeric check
#        as we have float numbers: we have to admint the decimal point as valid character and
#        remove it before the numeric check
#
#

def mapFields(lin,lctr):

   global maxtmp, mintmp, maxhum, minhum, maxpres, minpres, maxwind, maxrain
   global wtmp,whum,wpres,wwind,wrain
   global errctr

#------------------------------------------------------------------------------
#   Important part: numeric check as explained above
#------------------------------------------------------------------------------

   li=lin.split("\t")
   if (li[5].lstrip('-').replace('.','',1).isdigit()):
      wtmp=float(li[5])
   else:
      print "Temperature value not numeric in line: ",lctr
      errctr+=1

   if (li[6].replace('.','',1).isdigit()):
       whum=float(li[6])
   else:
      print "Humidity value not numeric in line: ",lctr 
      errctr+=1 
          
   if (li[7].replace('.','',1).isdigit()):
       wpres=float(li[7])
   else:
      print "Pressure value not numeric in line: ",lctr
      errctr+=1
          
   if (li[10].replace('.','',1).isdigit()):
     wwind=float(li[10])
   else:
      print "Wind gust value not numeric in line: ",lctr
      errctr+=1
           
   if (li[15].replace('.','',1).isdigit()):
     wrain=float(li[15])
   else:
      print "24 hour rain value not numeric in line: ",lctr
      errctr+=1
#
#    set maximal and minimal values as required
#
   if wtmp>maxtmp:
     maxtmp=wtmp
   if wtmp<mintmp:
     mintmp=wtmp
   if whum>maxhum:
     maxhum=whum
   if whum<minhum:
     minhum=whum
   if wpres>maxpres:
     maxpres=wpres
   if wpres<minpres:
     minpres=wpres
   if maxwind<wwind:
     maxwind=wwind
   if maxrain<wrain:
     maxrain=wrain
#
#	End of mapFields(lin)
#------------------------------------------------------------------------------


#
#       createStrg() creates a string object per day with minimal / maximal values
#

def createStrg(wdat):

   global maxtmp, mintmp, maxhum, minhum, maxpres, minpres, maxwind, maxrain
   c=',' 

   lstr=wdat+c+str(maxtmp)+c+str(mintmp)+c+str(maxhum)+c+str(minhum)+c+ \
        str(maxpres)+c+str(minpres)+c+str(maxwind)+c+str(maxrain)
   return lstr
#
#	End of createStrg()
#------------------------------------------------------------------------------


#
#       createQuery(litem) creates a query string for MariaDB insert
#

def createQuery(litem):

   global maxtmp, mintmp, maxhum, minhum, maxpres, minpres, maxwind, maxrain
   c=','

   cmd="INSERT INTO wdata (wdate,tempmax,tempmin,humidmax,humidmin \
       ,relpressmax,relpressmin,windmax,rainday) VALUES (" + litem \
       +")" 
   return cmd
#
#       End of createQuery(lineitem)
#------------------------------------------------------------------------------


#
#       createPrnStrg(li) creates a formated print string from a list
#

def createPrnStrg(li):

   spc=" "
   strg=""
    
   s1=str(li[0])+spc+str('{:5.1f}'.format(float(li[1])))+spc+str('{:5.1f}'.format(float(li[2])))+spc
   s2=str('{:6.1f}'.format(float(li[3])))+spc+str('{:6.1f}'.format(float(li[4])))+spc
   s3=str('{:7.1f}'.format(float(li[5])))+spc+str('{:7.1f}'.format(float(li[6])))+spc
   s4=str('{:6.1f}'.format(float(li[7])))+spc+str('{:6.1f}'.format(float(li[8])))
   strg=s1+s2+s3+s4
   return strg
#
#       End of createQuery(lineitem)
#------------------------------------------------------------------------------


#=====================  End of Script ==========================================
