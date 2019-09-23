#!/usr/bin/env python3

import os
import sys
import yaml
import datetime
import argparse
import concurrent.futures
from   fabric import Connection
import pdb
import random
import string
import mmap
import logging
from   itertools import repeat

def run_ssh(Host, User, Script, RunId, ImportOsCheckLib, OsReleasePath):

    UserHost = User+'@'+Host
    Command = ''

    if Script:
        #pdb.set_trace()
        lettersAndDigits = string.ascii_letters + string.digits
        RandomPart = ''.join(random.choice(lettersAndDigits) for __ in range(8))
        TmpScript = '/tmp/pybroadexec_'+RunId+'_'+RandomPart+'.sh'
        Connection(UserHost).put(Script, TmpScript)

        if ImportOsCheckLib:
            TmpOsLib = '/tmp/osrelease_lib_'+RunId+'_'+RandomPart+'.sh'
            Connection(UserHost).put(OsReleasePath, TmpOsLib)
            Command += \
                    'chmod 700 '+TmpOsLib+';\
                    sed -i -e \'/#!/r '+TmpOsLib+'\' '+TmpScript+';\
                    rm '+TmpOsLib+'; '

        Command += \
                'chmod 700 '+TmpScript+';\
                '+TmpScript+';\
                rm '+TmpScript

    Result = Connection(UserHost).run(Command, hide=True)
    #if Cfg['Settings']['OutputHostnameDelimiter']:
    #    OutputHostnameDelimiter = Cfg['Settings']['OutputHostnameDelimiter']
    #else:
    #    OutputHostnameDelimiter = ' '
    #if args.human_readable:
    #    Msg = "{0.connection.host}"+OutputHostnameDelimiter+"\n{0.stdout}"
    #else:
    #    Msg1 = "{0.connection.host}"+OutputHostnameDelimiter+"{0.stdout}"
    #    Msg = Msg1.replace('\n', ' ')
    Msg = "{0.connection.host}\n{0.stdout}"
    return Msg.format(Result)

def main():

    # go to PyBroadexec directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    with open ("config.yml", 'r') as Stream:
        try:
            Cfg = yaml.safe_load(Stream)
        except yaml.YAMLError as Exc:
            print(Exc)
        Stream.close()
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--script',           action="store",
        help='script path')
    parser.add_argument('-f', '--filter',           action="store",
        help='hostlist filter')
    parser.add_argument('-q', '--quiet',            action="store_true",
        help='be quiet')
    parser.add_argument(      '--external',         action="store",
        help='path to stats file in external run')
    parser.add_argument(      '--runid',            action="store",
        help='external runid')
    parser.add_argument('-r', '--report_path',      action="store",
        help='report path')
    parser.add_argument('-e', '--human_readable',   action="store_true",
        help='human readable output')
    parser.add_argument('-g', '--grep',             action="store",
        help='grep phrase from output')
    parser.add_argument('-i', '--case_insensitive', action="store_true",
        help='case insensitive for grep')
    parser.add_argument('-u', '--user',             action="store",
        help='define username')
    parser.add_argument('-a', '--admin',            action="store_true",
        help='enter admin functions')
    parser.add_argument('-l', '--list',             action="store",
        help='path to hostlist')
    parser.add_argument(      '--exclude',          action="store_true",
        help='exclude selected hosts')
    parser.add_argument('-H', '--hosts',            action="store",
        help='define hosts where to run')
    parser.add_argument('-c', '--copy_file',        action="store",
        help='file to be copied')
    parser.add_argument('-d', '--destination',      action="store",
        help='destination for file to be copied')
    parser.add_argument('-b', '--batch',            action="store_true",
        help='run in batch mode without user interaction')
    parser.add_argument(      '--version',          action="store_true",
        help='display broadexec version')
    args = parser.parse_args()
    
    if args.version:
        print('PyBroadexec version 0.1')
        sys.exit(0)
    
    if args.filter and (not args.list):
        raise Exception('Filter is specified but hostlist is missing. Use -l [HOSTLIST] or remove filter.')
    if args.hosts and args.list:
        raise Exception('-l, use hostlist, is not compatible with -H, defined hosts.')
    if args.human_readable and args.grep:
        raise Exception('-e, human readable, is not compatible with -g, grep option.')
    if args.case_insensitive and (not args.grep):
        raise Exception('-i, case insensitive, can be used only with -g, grep option.')
    if args.batch and args.admin:
        raise Exception('-b, batch mode, is not compatible with -a, admin mode.')
    if args.destination and (not args.copy_file):
        raise Exception('Destination is specified but file to be copied is missing.')
    if args.copy_file and args.admin:
        raise Exception('-c, copy file is not compatible with -a, admin mode.')
    if args.copy_file and args.script:
        raise Exception('-c, copy file is not compatible with -s, script path.')
    if (not args.user) and (not Cfg['Main']['User']):
        raise Exception('User not provided in config or via -u parameter.')
    
    
    Now = datetime.datetime.now()
    RunId = Now.strftime("%Y%m%d%H%M%S")+'_'+str(os.getpid())
    if args.runid:
        RunId = args.runid
    
    if Cfg['Path']['Logs']:
        LogPath = Cfg['Path']['Logs']
    else:
        LogPath = './logs'
    
    if not os.path.isdir(LogPath):
        try:
            os.mkdir(LogPath)
        except OSError:
            print("Unable to create %s directory" % LogPath)
    
    StatsDir = (LogPath+'/stats')
    if not os.path.isdir(StatsDir):
        try:
            os.mkdir(StatsDir)
        except OSError:
            print("Unable to create %s directory" % StatsDir)
    
    LogLastRun =      LogPath+'/brdexec_last_run.log'
    LogCheckUpdates = LogPath+'/brdexec_check_updates.log'
    LogFile =         LogPath+'/brdexec.log'
    
    if os.path.isfile(LogLastRun):
        os.remove(LogLastRun)
    
    if Cfg['Path']['Reports']:
        ReportPath = Cfg['Path']['Reports']
    elif args.report_path:
        ReportPath = args.report_path
    else:
        ReportPath = './reports'
    
    
    #FIXME future code
    #if args.external:
    #    if os.path.isfile(args.external):
    #        StatsFile = args.external
    #    else:
    #        StatsPath = Cfg['Path']['Logs']+'/stats/'+RunId
    #        if os.path.isfile(StatsPath):
    #            os.remove(StatsPath)
    #        StatsFile = open(StatsPath,"w+")
    #        StatsFile.write("STATE INIT\nPROGRESS NULL NULL NULL NULL\n")
    #        StatsFile.close()
    
    if args.hosts:
        Hosts = args.hosts.split(",")
    elif args.list:
        Hosts = [line.rstrip('\n') for line in open(args.list)]
    
    try:
        Hosts
    except NameError:
        print('ERROR: No hosts defined. Exiting...')
        sys.exit(1)
    
    with open(args.script) as Stream:
        s = mmap.mmap(
                Stream.fileno(), 
                0, 
                access=mmap.ACCESS_READ
                )
        if s.find(b'osrelease_check') != -1:
            ImportOsCheckLib = 'true'
    Stream.close()
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        Results = executor.map(
                run_ssh, 
                Hosts, 
                repeat(Cfg['Main']['User']), 
                repeat(args.script), 
                repeat(RunId), 
                repeat(ImportOsCheckLib), 
                repeat(Cfg['Path']['OsReleaseLib'])
                )
        for result in Results:
            print(result)

if __name__ == "__main__":
    main()
