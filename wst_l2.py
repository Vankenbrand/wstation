#!/usr/bin/python
#
#	WSTLIB contains generic functions for the weatherstation program
#	Centralized in this Module
#	Author:	V Ankenbrand
#	Date:	17/12/2016
#
 
import subprocess, string, os, time, mysql.connector

#------------------------------------------------------------------------------
#
#       Level 2 Functions
#
#------------------------------------------------------------------------------

#
#       getTstmp returns time stamp Day-Month-Year Hour:Minute
#

def getTstmp():
  "Gets current time from the time library"

  return time.strftime("%d-%m-%Y %H:%M")
#
#       End of getTstmp()
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#       getTime returns time stamp Hour:Minute:Second
#------------------------------------------------------------------------------


def getTime():
  "Gets current time from the time library"

  return time.strftime("%H:%M:%S")

#
#       END of getTime()
#------------------------------------------------------------------------------
#
#=====================  End of Script ==========================================
