#!/usr/bin/env python3

import os
import sys
import yaml
import datetime
import argparse
#import pdb
import random
import string
import mmap
#import logging
#from itertools import repeat
from consolemenu import *
from consolemenu.items import *
import asyncio
import asyncssh
import time

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    with open("config.yml", 'r') as Stream:
        try:
            Cfg = yaml.safe_load(Stream)
        except yaml.YAMLError as Exc:
            print(Exc)

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--script',           action="store",
                        help='script path')
    parser.add_argument('-f', '--filter',           action="store",
                        help='hostlist filter')
    parser.add_argument('-q', '--quiet',            action="store_true",
                        help='be quiet')
    parser.add_argument('--external',               action="store",
                        help='path to stats file in external run')
    parser.add_argument('--runid',                  action="store",
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
    parser.add_argument('--exclude',                action="store_true",
                        help='exclude selected hosts')
    parser.add_argument('-H', '--hosts',            action="store",
                        help='define hosts where to run')
    parser.add_argument('-c', '--copy_file',        action="store",
                        help='file to be copied')
    parser.add_argument('-d', '--destination',      action="store",
                        help='destination for file to be copied')
    parser.add_argument('-b', '--batch',            action="store_true",
                        help='run in batch mode without user interaction')
    parser.add_argument('--version',                action="store_true",
                        help='display broadexec version')
    args = parser.parse_args()

    if args.version:
        print('PyBroadexec version 0.1')
        sys.exit(0)

    if args.filter and (not args.list):
        raise Exception('Filter is specified but hostlist is missing. '\
                        'Use -l [HOSTLIST] or remove filter.')
    if args.hosts and args.list:
        raise Exception('-l, use hostlist, is not compatible with -H, '\
                        'defined hosts.')
    if args.human_readable and args.grep:
        raise Exception('-e, human readable, is not compatible with -g, '\
                        'grep option.')
    if args.case_insensitive and (not args.grep):
        raise Exception('-i, case insensitive, can be used only with -g, '\
                        'grep option.')
    if args.batch and args.admin:
        raise Exception('-b, batch mode, is not compatible with -a, '\
                        'admin mode.')
    if args.destination and (not args.copy_file):
        raise Exception('Destination is specified but file to be copied '\
                        'is missing.')
    if args.copy_file and args.admin:
        raise Exception('-c, copy file is not compatible with -a, admin mode.')
    if args.copy_file and args.script:
        raise Exception('-c, copy file is not compatible with -s, '\
                        'script path.')
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

    LogLastRun = LogPath+'/brdexec_last_run.log'
    LogCheckUpdates = LogPath+'/brdexec_check_updates.log'
    LogFile = LogPath+'/brdexec.log'

    if os.path.isfile(LogLastRun):
        os.remove(LogLastRun)

    if Cfg['Path']['Reports']:
        ReportPath = Cfg['Path']['Reports']
    elif args.report_path:
        ReportPath = args.report_path
    else:
        ReportPath = './reports'

    if Cfg['Settings']['OutputHostnameDelimiter']:
        OutputHostnameDelimiter = Cfg['Settings']['OutputHostnameDelimiter']
    else:
        OutputHostnameDelimiter = ' '

    # FIXME future code
    # if args.external:
    #     if os.path.isfile(args.external):
    #         StatsFile = args.external
    #     else:
    #         StatsPath = Cfg['Path']['Logs']+'/stats/'+RunId
    #         if os.path.isfile(StatsPath):
    #             os.remove(StatsPath)
    #         StatsFile = open(StatsPath,"w+")
    #         StatsFile.write("STATE INIT\nPROGRESS NULL NULL NULL NULL\n")
    #         StatsFile.close()

    if args.hosts:
        Hosts = args.hosts.split(",")
    elif args.list:
        Hosts = [line.rstrip('\n') for line in open(args.list)]
    else:
        hosts_list = []
        for root, dirs, files in os.walk(Cfg['Path']['HostsDir']):
            for file in files:
                hosts_list.append(file)
        hosts_list_selection = SelectionMenu.get_selection(hosts_list)
        hosts_file = Cfg['Path']['HostsDir']+'/'+hosts_list[hosts_list_selection]
        Hosts = [line.rstrip('\n') for line in open(hosts_file)]

    try:
        Hosts
    except NameError:
        print('ERROR: No hosts defined. Exiting...')
        sys.exit(1)


    ### SCRIPT
    cwd = os.getcwd()
    if not args.script:
        scripts = []
        for root, dirs, files in os.walk(Cfg['Path']['ScriptsDir']):
            for file in files:
                if file.endswith('.sh'):
                    scripts.append(file)
        script_selection = SelectionMenu.get_selection(scripts)
        script = cwd+'/'+Cfg["Path"]["ScriptsDir"]+'/'+scripts[script_selection]
    else:
        script = cwd+'/'+Cfg["Path"]["ScriptsDir"]+'/'+args.script

    with open(script) as Stream:
        s = mmap.mmap(
                Stream.fileno(),
                0,
                access=mmap.ACCESS_READ
                )
        if s.find(b'osrelease_check') != -1:
            ImportOsCheckLib = 'true'
        else:
            ImportOsCheckLib = 'false'

    HumanReadable = False
    if args.human_readable:
        HumanReadable = args.human_readable
    elif Cfg['Settings']['HumanReadableReport']:
        HumanReadable = Cfg['Settings']['HumanReadableReport']

    async def run_client(Host, TmpScript, Command):
        async with asyncssh.connect(Host) as conn:
            await asyncssh.scp((conn, script), TmpScript)
            result = await conn.run(Command)


            if result.exit_status == 0:
                if HumanReadable:
                    print(Host + OutputHostnameDelimiter + '\n' + result.stdout, end='')
                else:
                    print(Host+OutputHostnameDelimiter+result.stdout.replace('\n', ' '), end='')
            else:
                print(result.stderr, end='', file=sys.stderr)
                print('Program exited with status %d' % result.exit_status,
                      file=sys.stderr)

    def run_ssh():
        UserHost = Cfg['Main']['User'] + '@' + Host
        Command = ''
        lettersAndDigits = string.ascii_letters + string.digits
        RandomPart = ''.join(random.choice(lettersAndDigits) for __ in range(8))

        if script:
            # pdb.set_trace()
            TmpScript = '/tmp/pybroadexec_' + RunId + '_' + RandomPart + '.sh'

            if ImportOsCheckLib:
                TmpOsLib = '/tmp/osrelease_lib_' + RunId + '_' + RandomPart + '.sh'
                Command += \
                    'chmod +x ' + TmpOsLib + ';\
                    sed -i -e \'/#!/r ' + TmpOsLib + '\' ' + TmpScript + ';\
                    rm ' + TmpOsLib + '; '

            Command += \
                'chmod +x ' + TmpScript + ';\
                ' + TmpScript + ';\
                rm ' + TmpScript

        try:
            asyncio.get_event_loop().run_until_complete(run_client(Host, TmpScript, Command))
        except (OSError, asyncssh.Error) as exc:
            sys.exit('SSH connection failed: ' + str(exc))


    for Host in Hosts:
        run_ssh()

if __name__ == "__main__":
    main()
