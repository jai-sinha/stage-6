#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr,  // this and below are selection conditions
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
	cout << "Doing QU_Select " << endl;

	// Make sure to give ScanSelect the proper input
	Status status;
	AttrDesc attrDescArr[projCnt];
	AttrDesc *attrDesc = NULL;
	int reclen;

	// get info for all attributes in projNames, and total reclen
	for (int i=0; i<projCnt; i++) {
		// To go from attrInfo to attrDesc, need to consult the catalog (attrCat and relCat, global variables)
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDescArr[i]);
		if (status != OK) return status;

		reclen += attrDescArr[i].attrLen;
	}

	// get attr info, or leave if null
	if (attr != NULL) {
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
		if (status != OK) return status;
	}

	// scanSelect call
	return ScanSelect(result, projCnt, attrDescArr, attrDesc, op, attrValue, reclen);
}

const Status ScanSelect(const string & result, // table to store output
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[], // see below for desc of AttrDesc
			const AttrDesc *attrDesc, // attr for selection
			const Operator op, 
			const char *filter, //attrValue
			const int reclen) // length of output tuple
{
   cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	
	Status status;
	// have a temporary record for output table
	Record outputRec;
	Record tmpRec;
	RID rid;
	RID outRID;
	
	outputRec.length = reclen;
	outputRec.data = (char *)malloc(reclen);

	// open "result" as an InsertFileScan object
	InsertFileScan resultRel(result, status);
	if (status != OK) return status;

	// open current table (to be scanned) as a HeapFileScan object
	HeapFileScan scan(projNames[0].relName, status);
	if (status != OK) return status;

	// check if an unconditional scan is required
	int type;
	if (attrDesc == NULL) type = 0;
	else type = attrDesc->attrType;

	if (type == 0) status = scan.startScan(0, 0, STRING, NULL, EQ);

	// check attrType: INTEGER, FLOAT, STRING
	else if (type == STRING) {
		status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, EQ);
	}

	else if (type == FLOAT) {
		float f = atof(filter);
		status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, (char *)&f, EQ);
	}

	else if (type == INTEGER) {
		int i = atoi(filter);
		status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, (char *)&i, EQ);
	}

	if (status != OK) return status;
	
	// scan the current table
	while (scan.scanNext(rid) == OK) {
		status = scan.getRecord(tmpRec);
		if (status != OK) return status;

		// if find a record, then copy stuff over to the temporary record (memcpy)
		// track outputRec offset as we add records to it
		int offset = 0;
		for (int i=0; i<projCnt; i++) {

			type = projNames[i].attrType;
			if (type == STRING) {
				memcpy((char *)outputRec.data + offset, (char *)tmpRec.data + projNames[i].attrOffset, projNames[i].attrLen);
			} else if (type == FLOAT) {
				memcpy((char *)outputRec.data + offset, (float *)tmpRec.data + projNames[i].attrOffset, projNames[i].attrLen);
			} else if (type == INTEGER) {
				memcpy((char *)outputRec.data + offset, (int *)tmpRec.data + projNames[i].attrOffset, projNames[i].attrLen);
			}

			offset += projNames[i].attrLen;
		}

		status = resultRel.insertRecord(outputRec, outRID);
	}
	// insert into the output table

	if (status != FILEEOF) return status;
	
	status = scan.endScan();
	if (status != OK) return status;

	return OK;
}

