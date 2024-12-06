#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
	Status status;
	HeapFileScan* hfs;

	// setting up the scans
	if (relation.empty() || attrName.empty()) return BADCATPARM;

	hfs = new HeapFileScan(RELCATNAME, status);// maybe I need to pass in ATTRCATNAME
	if (status != OK) return status;

	// starting a scan to locate the qualifying tuples in the attr table
	if ((status = hfs->startScan(0, relation.length() + 1, type, attrValue, op)) != OK) {
		delete hfs;
		return status;
	}

	// tempory record to iterate over
	RID rid;
	Record rec;
	
	// temporary record to check against
	AttrDesc record;

	// looping through the attributes catalogue and deleting until no more qualifying records found 
	while(status == OK) {
		while((status = hfs->scanNext(rid)) == OK) {
			if ((status = hfs->getRecord(rec)) != OK) return status;

			assert(sizeof(AttrDesc) == rec.length);
			memcpy(&record, rec.data, rec.length);

			if (string(record.attrName) == attrName) break;
		}

		if (status == OK) {
			status = hfs->deleteRecord();
			// not going to return status now to prevent memory leak
		}
	}
	Status s0 = status; // return status of the exit loop
	// free the memory
	hfs->endScan();
	delete hfs;
	if (status != NORECORDS) return status;
	// make sure loop terminated because we it hit the end of the file
	if(s0 != FILEEOF) return s0;

	// reached end of file, should have deleted all tuples from relation
	return OK;
}


