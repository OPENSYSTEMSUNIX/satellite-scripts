#!/usr/bin/python
#sync spacewalk repos and clean up old and orphaned packages
#repo names can be found in ./repos
#options can be found in ./options.py

import xmlrpclib
import httplib
import datetime
import ConfigParser
import optparse
import sys
import os
import options
import subprocess

from optparse import OptionParser

###########################################################
#functions
def cmp_dictarray(pkgs, id):
    for pkg in pkgs:
        for (key,val) in pkg.iteritems():
            if val == id:
                return True
    return False

def get_delta(spacewalk, spacekey, repo):
    latest = spacewalk.channel.software.listLatestPackages(spacekey, repo)
    allpkgs = spacewalk.channel.software.listAllPackages(spacekey, repo)
    full_delta = []
    delta = []
    print " - Amount: %d" % len(latest)
    print " - Amount currently: %d" % len(allpkgs)
    for pkg in allpkgs:
        if not cmp_dictarray(latest, pkg['id']):
            print "Marked:  %s-%s-%s (id %s)" % (pkg['name'], pkg['version'], pkg['release'], pkg['id'])
            full_delta.append(pkg)
            delta.append(pkg['id'])
    return delta

def sync_pkgs(repo):
    print "=========================================================="
    print "Attempting to sync channel label: %s" % repo
    cmd = ['/usr/bin/spacewalk-repo-sync', '--channel', repo, '--type', 'yum', '--latest']
    proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    ret = proc.returncode
    if ret > 0:
         print "stderr: %s" % err

def print_label(spacewalk, spacekey):
    for i in spacewalk.channel.listAllChannels(spacekey):
        print i['label']

def delete_delta(spacewalk, spacekey, repo, delta):
    spacewalk.channel.software.removePackages(spacekey, repo, delta)

def main():
    print "Starting repo sync and cleanup on datetime.datetime.now()"

    #open list of repos
    repos = open(options.repo_file, 'r')
    myrepos = repos.read().splitlines()
    repos.flush()

    #open session with spacewalk api
    print "Connecting to api..."
    spacewalk = xmlrpclib.Server("https://%s/rpc/api" % options.spaceserver, verbose=0)
    spacekey = spacewalk.auth.login(options.spw_user, options.spw_pass)
    print "Connection established."

    #iterate through channel labels and do work
    for repo in myrepos:
        #attempt to sync channel
        #sync_pkgs(repo)
        print "Cleaning up channel lablel: %s" % repo
        #get array of RPM ids to clean up
        delta = get_delta(spacewalk, spacekey, repo)
        if len(delta) < 1:
            print "no packages to delete"
            print "=========================================================="
        else:
            print "deleting marked packages..."
            delete_delta(spacewalk, spacekey, repo, delta)
            print "=========================================================="

    #close repo list file
    repos.close()
    #logout of api
    spacewalk.auth.logout(spacekey)
    print "Completed repo sync and clean up on datetime.datetime.now(). Exiting now."

###########################################################
#main
if __name__ == "__main__":
    main()