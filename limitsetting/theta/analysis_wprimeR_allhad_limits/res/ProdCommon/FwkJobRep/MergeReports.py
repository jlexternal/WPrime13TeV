#!/usr/bin/env python
"""
_MergeReports_

Given two FrameworkJobReport XML files, concatenate them into
a single file.

"""
import os, time
from IMProv.IMProvDoc import IMProvDoc
from ProdCommon.FwkJobRep.ReportParser import readJobReport




def mergeReports(reportFile1, reportFile2):
    """
    _mergeReports_

    Load job reports from both files, and combine them into a
    single file.

    The output will be written to the first file provided.
    (IE JobReports from reportFile2 will be added to reportFile1)

    If reportFile1 does not exist, a new report will be created, containing
    the contents of reportFile2.
    
    If reportFile2 does not exist, then a RuntimeError is thrown.

    """
    if not os.path.exists(reportFile1):
        reports1 = []
    else:
        reports1 = readJobReport(reportFile1)

    if not os.path.exists(reportFile2):
        msg = "Report file to be merged does not exist:\n"
        msg += reportFile2
        raise RuntimeError, msg

    reports2 = readJobReport(reportFile2)
    reports1.extend(reports2)

    
    output = IMProvDoc("JobReports")
    for item in reports1:
        output.addNode(item.save())
    handle = open(reportFile1, 'w')
    handle.write(output.makeDOMDocument().toprettyxml())
    handle.close()
    return



def updateReport(reportFile, newReportInstance):
    """
    _updateReport_

    Given a file containing several reports: reportFile,
    find the report in there whose name matches the newReportInstance's
    name and replace that report with the new Report instance.

    Returns a boolean: True if report name was matched and updated,
    False if the report was not found and updated. (False may indicate that
    the new report file needs to be merged with the main report file)

    """
    if not os.path.exists(reportFile):
        existingReports = []
    else:
        existingReports = readJobReport(reportFile)

    updatedReport = False
    output = IMProvDoc("JobReports")
    for report in existingReports:
        if report.name == newReportInstance.name:
            output.addNode(newReportInstance.save())
            updatedReport = True
        else:
            output.addNode(report.save())

    
    handle = open(reportFile, 'w')
    handle.write(output.makeDOMDocument().toprettyxml())
    handle.close()
    return updatedReport


def combineReports(reportFile, reportNames, newReportInstance):
    """
    Combine reports, take some fields from report with given name
    in reportFile and then overwrite with newReportInstance
    
    Note: newReportInstance is modified, and should be written back as the 
    task fjr - else subsequent tasks will take the wrong one!!!
    
    """
    if not os.path.exists(reportFile):
        existingReports = []
    else:
        existingReports = readJobReport(reportFile)

    if not isinstance(reportNames, list):
        reportNames = [reportNames]

    reportFound = False
    output = IMProvDoc("JobReports")
    
    #wipe old values ready for new ones
    newReportInstance.inputFiles = []
    newReportInstance.generatorInfo = {} #how to handle multiple?
    
    for report in existingReports:
        if report.name in reportNames:
            reportFound = True
            
            # copy some values across from old report
            newReportInstance.inputFiles.extend(report.inputFiles)
            newReportInstance.skippedEvents.extend(report.skippedEvents)
            newReportInstance.skippedFiles.extend(report.skippedFiles)
    
            # loop over output files and change provenance to 1st node's
            for outfile in newReportInstance.files:
                oldinputfiles = outfile.inputFiles
                outfile.inputFiles = [] #clear ready for correct provenance info
                for infile in oldinputfiles:
                    # find the ancestor input files in previous report
                    for ancestor in report.files:
                        if ancestor['LFN'] == infile['LFN']:
                            outfile.inputFiles.extend(ancestor.inputFiles)
                            print "Updated InputFiles %s for %s" % (
                                ancestor.inputFiles, outfile['LFN'])
                        # No LFN, use PFN (Needed for parent forwarding)
                        elif not infile['LFN'] and \
                                ancestor['PFN'] == infile['PFN']:
                            outfile.inputFiles.extend(ancestor.inputFiles)
                            print "Updated InputFiles %s for %s" % (
                                ancestor.inputFiles, outfile['LFN'])
                       
            if report.timing.has_key('AppStartTime') and \
                report.timing['AppStartTime'] < newReportInstance.timing.get('AppStartTime', time.time()):
                newReportInstance.timing['AppStartTime'] = report.timing['AppStartTime']
            continue
        
        #  // if here either this report is not one of the inputs
        # //     or the report contained a staged out file
        #//            - in either case it must be saved
        output.addNode(report.save())
    
    if not reportFound:
        raise RuntimeError, "Reports not combined: %s not found in %s" % \
                        (str(reportNames), reportFile)
    
    output.addNode(newReportInstance.save())
    
    handle = open(reportFile, 'w')
    handle.write(output.makeDOMDocument().toprettyxml())
    handle.close()
    return newReportInstance
