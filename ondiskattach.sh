#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"

GETMAILRECIPIENT_SCRIPT=$DIR/getmailrecipient.py
GETMAILSENDERADDR_SCRIPT=$DIR/getmailsenderaddr.py
GETMAILSENDERNAME_SCRIPT=$DIR/getmailsendername.py

RECIPIENT_ADDRESS="$( $GETMAILRECIPIENT_SCRIPT )"

if [ $? -eq 1 ]
then
    echo "Error executing $GETMAILRECIPIENT_SCRIPT to get the recipient e-mail address from the configuration file!"
    exit 1
fi

SENDER_ADDRESS="$( $GETMAILSENDERADDR_SCRIPT )"

if [ $? -eq 1 ]
then
    echo "Error executing $GETMAILSENDERADDR_SCRIPT to get the sender e-mail address from the configuration file!"
    exit 1
fi

SENDER_NAME="$( $GETMAILSENDERNAME_SCRIPT )"

if [ $? -eq 1 ]
then
    echo "Error executing $GETMAILSENDERNAME_SCRIPT to get the sender name from the configuration file!"
    exit 1
fi

$DIR/backup.py -bs -a mail $1 2>&1 | mail -a "From: $SENDER_NAME <$SENDER_ADDRESS>" -s "$1 result" $RECIPIENT_ADDRESS
