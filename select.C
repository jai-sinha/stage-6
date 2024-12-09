#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

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
	AttrDesc *attrDesc = nullptr;
	int reclen = 0;

	// get info for all attributes in projNames, and total reclen
	for (int i=0; i<projCnt; i++) {
		attrDesc = new AttrDesc;
		// To go from attrInfo to attrDesc, need to consult the catalog (attrCat and relCat, global variables)
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDescArr[i]);
		if (status != OK) {
			delete attrDesc;
			return status;
		}
		reclen += attrDescArr[i].attrLen;
	}

	// get attr info, or leave if null
	if (attr != nullptr) {
      attrDesc = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
		if (status != OK) {
			delete attrDesc;
			return status;
		}
	}

	// scanSelect call
	status = ScanSelect(result, projCnt, attrDescArr, attrDesc, op, attrValue, reclen);
    if (attrDesc != nullptr) {
        delete attrDesc;
    }
    return status;
}

const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[], 
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter, 
			const int reclen) 
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
	if (status != OK){
		free(outputRec.data);
		return status;
	}

	// open current table (to be scanned) as a HeapFileScan object
	HeapFileScan scan(projNames[0].relName, status);
	if (status != OK) {
		free(outputRec.data);
		return status;
	}

	// check if an unconditional scan is required
	if (attrDesc == nullptr) {
		status = scan.startScan(0, 0, STRING, NULL, EQ);
    } else {
        switch (attrDesc->attrType) {
            case STRING:
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
                break;
            case FLOAT: {
                *((float*) filter) = atof(filter);
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, filter, op);
                break;
            }
            case INTEGER: {
                *((int*) filter) = atoi(filter);
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, filter, op);
                break;
        }
    }
	}

	if (status != OK) {
      free(outputRec.data);
		return status;
	}
	
	// scan the current table
	while (scan.scanNext(rid) == OK) {
		status = scan.getRecord(tmpRec);
		if (status != OK) return status;

		// if find a record, then copy stuff over to the temporary record (memcpy)
		// track outputRec offset as we add records to it
		int offset = 0;
		for (int i=0; i<projCnt; i++) {
            memcpy((char *)outputRec.data + offset, (char *)tmpRec.data + projNames[i].attrOffset, projNames[i].attrLen);
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
