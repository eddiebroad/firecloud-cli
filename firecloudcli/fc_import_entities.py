#!/usr/bin/env python

from methods_repo import httpRequest,read_entire_file

import argparse
import urllib
import sys



def getNumLinesInFile(fp):
	numLines=0
	reader=open(fp,'r')
	for line in reader:
		numLines=numLines+1
	reader.close()
	return numLines


def chunkBigTSVFile(bigFile,chunkSize):
	lineNum=0
	headerLine=None
	reader=open(bigFile,'r')
	chunkID=0
	chunkWriter=None
	chunkFiles=list()
	for line in reader:
		if(lineNum==0):
			headerLine=line
		else:
			if(lineNum==1 or lineNum%chunkSize==0):
				chunkFile=bigFile+"_chunk_"+str(chunkID)
				chunkFiles.append(chunkFile)
				chunkID=chunkID+1
				chunkWriter=open(chunkFile,'w')
				if(chunkWriter is None):
					raise Exception("Error in opening file "+chunkFile+"!")
				chunkWriter.write(headerLine)
			chunkWriter.write(line)
		lineNum=lineNum+1
	return chunkFiles




def uploadEntities(fcOrc,name,namespace,entityFile,enableChunking=False,chunkSize=200):
	linesInFile=getNumLinesInFile(entityFile)
	maxSize=250
	chunkSize=200
	if(linesInFile>maxSize and enableChunking):
		chunkFiles=chunkBigTSVFile(entityFile,200)
		for chunkFile in chunkFiles:
			print "Got file ",chunkFile," to upload!"
			uploadEntities(fcOrc,name,namespace,chunkFile)
		return
	method="POST"
	base="https://"+fcOrc
	path="/service/api/workspaces/"+namespace+"/"+name+"/importEntities"
	#disable strip here because some columns in a TSV at the right end could be empty
	#and stripping those lines will cause column mistmatch
	entityData=read_entire_file(entityFile,False)
	entityDict= {"entities": entityData}
	requestBody=urllib.urlencode(entityDict)
	expectedReturnStatus=200
	insecureSsl=False
	useFormHeader=True
	response=httpRequest(base, path, insecureSsl, method, requestBody, expectedReturnStatus,useFormHeader)


def main():
	descStr="Utility for uploading TSV ENTITY_FILEs to firecloud."
	parser=argparse.ArgumentParser(description=descStr)
	parser.add_argument('-FC_ORCH_URL',default="firecloud.dsde-prod.broadinstitute.org",
		help="The orchestration server (for example firecloud.dsde-alpha.broadinstitute.org)")
	parser.add_argument('-chunking',default=False,action="store_true",help="enable chunking of TSVs")
	parser.add_argument('-chunk_size',default=250,help="size of chunks in lines (>=100)")
	parser.add_argument('NAMESPACE',help='The workspace namespace')
	parser.add_argument('NAME',help="for the workspace name")
	parser.add_argument('ENTITY_FILE',help="path to the ENTITY_FILE TSV file")
	args=parser.parse_args()
	if(args):
		fcOrc=args.FC_ORCH_URL
		namespace=args.NAMESPACE
		name=args.NAME
		entityFile=args.ENTITY_FILE
		chunkingEnabled=args.chunking
		chunkSize=int(args.chunk_size)
		if(chunkSize<100):
			print "Error, chunk size too small! Abort !"
			import sys
			sys.exit(0)


		uploadEntities(fcOrc,name,namespace,entityFile,chunkingEnabled)
	else:
		parser.print_help()



if __name__ == "__main__":
    main()



