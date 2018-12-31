#!/bin/bash
LOG_FILE=/tmp/file-checker.log
IBM_CLI_TAR_FILE=IBM_Cloud_0.12.1_amd64.tar.gz
IBMCLOUD_CLI_BIN=""
UTILS_DIR=../Utils
CRN_CREDENTIALS_FILE=crn.txt
AKEY_SKEY_CREDENTIALS_FILE =askey.txt
function write_to_log
{
   echo $1 >> $LOG_FILE
}
function install_IBM_CLI
{
    write_to_log "Installing IBM CLI"
	
	IBMCLOUD_CLI_BIN=Bluemix_CLI/bin/ibmcloud
	if [ -f $IBMCLOUD_CLI_BIN ] ; then
	  write_to_log " IBM CLI already present. No need to install"
	else
	tar -zxvf $IBM_CLI_TAR_FILE
	write_to_log "Installation of IBM CLI completed successfully"
}
function install_cos_plugin
{
  returnCode = 0
  //to do check if plugin is available
  write_to_log "Installing IBM COS plugin"
  printf 'y' | $IBMCLOUD_CLI_BIN plugin install cloud-object-storage &> $LOG_FILE
  if [ $? -eq 0 ] ; then
  write_to_log "Successfully installed cloud-object-storage"
  else
  returnCode =1 
  write_to_log "Failed to install cloud-object-storage"
  fi
  exit $returnCode
}
function configure_cos_and_check_output
{
   returnCode=0
   $IBMCLOUD_CLI_BIN cos config < $CRN_CREDENTIALS_FILE &> $LOG_FILE
   $IBMCLOUD_CLI_BIN cos config --hmac < $AKEY_SKEY_CREDENTIALS_FILE &> $LOG_FILE 
   $IBMCLOUD_CLI_BIN cos config --switch hmac &> $LOG_FILE 
   $IBMCLOUD_CLI_BIN cos list-buckets | grep $OUTPUT_BUCKET | grep -v grep
   if [ $? -eq 0 ] ; then
  write_to_log "File Present.."
  else
  returnCode =1 
  write_to_log "File not present."
  fi
  exit $returnCode
      
}

//MAIN PROGRAM
write_to_log"Starting File Checker Test..."
// Check args to script
retCode = 0
if [ "$# -ne 4 ] ; then
 write_to_log "Usage file-checker.sh <Resource Instance ID CRN> <Access Key> <Secret Key> <output-bucket>
 retCode=1
 exit $retCode
 fi
 //Al action happens from here
 cd $UTILS_DIR
 RES_ID_CRN=$1
 ACCESS_KEY=$2
 SECRET_KEY=$3
 OUTPUT_BUCKET=$4
write_to_log "Dump the input passed to script"
write_to_log "Resource ID CRN = $RES_ID_CRN"
write_to_log "ACCESS KEY = $ACCESS_KEY"
write_to_log "SECRET KEY = $SECRET_KEY"
write_to_log "OUTPUT BUCKET = $OUTPUT_BUCKET"

write_to_log "Creating CRN credentials"
echo "$RES_ID_CRN" > $CRN_CREDENTIALS_FILE
echo "y" >> $CRN_CREDENTIALS_FILE

write_to_log "creating access and secret key file"
echo "$ACCESS_KEY" > $AKEY_SKEY_CREDENTIALS_FILE
echo $SECRET_KEY" >> $AKEY_SKEY_CREDENTIALS_FILE

install_IBM_CLI
install_cos_plugin
configure_cos_and_check_output
if [ $? -eq 0 ] ; then
  write_to_log "Test case Passed"
  else
  returnCode =1 
  write_to_log "Test case failed."
  fi
  exit $returnCode

 