#!/bin/bash
#
NAME=csbridge
if [ ! -d /var/log/${NAME} ]
then mkdir /var/log/${NAME}
fi
LOGFILE=/var/log/${NAME}/run.log
#ERRORFILE=/var/log/${NAME}/error.log
RUN=/usr/sbin/${NAME}.py
 
# Fork off node into the background and log to a file
#${RUN} >>${LOGFILE} 2>>${ERRORFILE} </dev/null &
${RUN} 2>>${LOGFILE} >>${LOGFILE} </dev/null &
 
# Capture the child process PID
CHILD="$!"

function finish {
# Your cleanup code here
	kill $CHILD
	while ps -p $CHILD > /dev/null
	do
	  sleep 1;
	done
}
 
# Kill the child process when start-stop-daemon sends us a kill signal
trap finish INT TERM
 
# Wait for child process to exit
wait

