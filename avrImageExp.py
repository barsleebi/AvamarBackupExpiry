import os, sys, re, string, time, paramiko
from operator import itemgetter
from typing import OrderedDict
from paramiko import SSHClient
from paramiko.util import retry_on_signal

avrServerName = sys.argv[1]

sys.stdout = open('script.log', 'w')
sys.stderr = open('error.log','w')

def ssh_connect(serverName,command):
    connectSSH = paramiko.SSHClient()
    connectSSH.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        connectSSH.connect(serverName,username="<username>",password="<password>")
        #Change server password in above line before executing the script
        stdin, stdout, stderr = connectSSH.exec_command(command)
        connectSSH.output = stdout.readlines()
        return connectSSH.output
    except:
        print("[!] Cannot connect to ", serverName, "please check connection")
    connectSSH.close()

def avr_snapup(serverName,cleanOutput):
    for domain in cleanOutput:
        pattern = '/'
        domain = domain.strip("\n")
        position = [pos for pos, char in enumerate(domain) if char==pattern]
        if len(position)>1:
            filename = domain[position[-1]:]
        else:
            filename = domain
        print("Step 3: creating snapup files in /tmp/snapups with name", filename)
        snapupsCommand = "modify-snapups --mode=delete --domain={} --before='15 days ago' >/tmp/snapups/{}_BackupExp &".format(domain,filename)
        #As per this script it will expire anything older than 15 days, feel free to change the date range as per your need
        #Also you can refer below KB URL provided by EMC to change the modify-snapups command as per your requirement.
        #https://www.dell.com/support/kbdoc/en-uk/000058216/avamar-capacity-management-how-to-delete-or-expire-backups-in-bulk-with-the-modify-snapups-tool
        print("Executing ",snapupsCommand, " on ", serverName)
        ssh_connect(serverName,snapupsCommand)

def avr_backupexp(serverName):
    sshProcessCount = ssh_connect(serverName,"ps -ef | grep modify-snapups | wc -l")
    result = int(itemgetter(0)(sshProcessCount))
    if result > 1:
        print("Step 4: waiting for snapup to complete", result, "number of process left")
        status = True
        return status
    else:
        print("Step 5: Executing all scripts in /tmp/snapups")
        ssh_connect(serverName,"chown +x /tmp/snapups/*")
        time.sleep(1)
        ssh_connect(serverName,"find /tmp/snapups/ -maxdepth 1 -type f -executable -name '*_BackupExp' -exec {} \; &")
        time.sleep(1)
        sshProcessCount = ssh_connect(serverName,"uptime")
        result = int(itemgetter(0)(sshProcessCount))
        print("Step 6: Time to clear some space,", result," workers are on this task.")
        print("Final Step : Backup images will be completly cleared once GC process kicks in")
        status = False
        return status


        

status = True
if type(avrServerName) == type(None):
    print("Incorrect server name please check again")
else:
    print("Step 1: Collecting domain name for ", avrServerName)
    sshOutput = ssh_connect(avrServerName,"mccli domain show --recursive=true | awk 'FNR > 3 {print $2}'")
    sshOutput = list(OrderedDict.fromkeys(sshOutput)) #removing deuplicated in the list
    print("Step 2: Starting snapup command generation")
    avr_snapup(avrServerName,sshOutput)
    while(status):
        status = avr_backupexp(avrServerName)
        print("Going to sleep for 300 sec, see you soon")
        time.sleep(2)

sys.stdout.close()
sys.stderr.close()
