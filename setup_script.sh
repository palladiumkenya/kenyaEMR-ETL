#!/bin/bash
# Script to backup MySQL databases

# Parent backup directory
backup_parent_dir="/var/backups/mysql"

#script directory

current_dir=$(pwd)
script_dir=$(dirname $0)

if [ $script_dir = '.' ]
then
script_dir="$current_dir"
fi
echo script_directory: ${script_dir}

# MySQL settings
mysql_user="root"
mysql_password="root"
mysql_base_database="openmrs"



# Read MySQL password from stdin if empty
if [ -z "${mysql_password}" ]; then
  echo -n "Enter MySQL ${mysql_user} password: "
  read -s mysql_password
  echo
fi

# Check MySQL password
echo exit | mysql --user=${mysql_user} --password=${mysql_password} -B 2>/dev/null
if [ "$?" -gt 0 ]; then
  echo "MySQL ${mysql_user} password incorrect"
  exit 1
else
  echo "MySQL ${mysql_user} password correct."
fi

mysql --user=${mysql_user} --password=${mysql_password} ${mysql_base_database} < "${script_dir}/DDL.sql" 

if [ "$?" -gt 0 ]; then
  echo "MYSQL encountered a problem while executing DDL script."
  exit 1
else
  echo "Successfully executed DDL script .........................."
fi

mysql --user=${mysql_user} --password=${mysql_password} ${mysql_base_database} < "${script_dir}/DML.sql" 

if [ "$?" -gt 0 ]; then
  echo "MYSQL encountered a problem while executing DML script."
  exit 1
else
  echo "Successfully executed DML script .........................."
fi

mysql --user=${mysql_user} --password=${mysql_password} ${mysql_base_database} < "${script_dir}/Scheduled_Updates.sql" 

if [ "$?" -gt 0 ]; then
  echo "MYSQL encountered a problem while executing Scheduled Updates script."
  exit 1
else
  echo "Successfully executed Scheduled Updates script .........................."
fi
