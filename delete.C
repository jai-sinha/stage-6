#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 *      OK on success
 *      an error code otherwise
 */

const Status QU_Delete(const string &relation, 
                       const string &attrName, 
                       const Operator op, 
                       const Datatype type, 
                       const char *attrValue) {
	
	cout << "Doing QU_Delete " << endl;
	if (relation.empty()) 
		return BADCATPARM;

	Status status;
	RID rid;

	// Handle "delete all" case (when attrName is empty)
	if (attrName.empty()) {
		HeapFileScan hfs(relation, status);
		if (status != OK) 
			return status;

		status = hfs.startScan(0, 0, STRING, NULL, EQ);  // Unconditional scan
		if (status != OK) 
			return status;

		// Delete all records
		while ((status = hfs.scanNext(rid)) == OK) {
			status = hfs.deleteRecord();
			if (status != OK) {
				hfs.endScan();
				return status;
			}
		}

		hfs.endScan();
		return (status == FILEEOF) ? OK : status;
	}

	// Get the attribute descriptor for the attribute name
	AttrDesc attrDesc;
	status = attrCat->getInfo(relation, attrName, attrDesc);
	if (status != OK) return status;

	// Open a HeapFileScan on the relation
	HeapFileScan hfs(relation, status);
	if (status != OK) return status;

	// Start scanning with the provided filter
	switch (attrDesc.attrType) {
            case STRING:
                status = hfs.startScan(attrDesc.attrOffset, attrDesc.attrLen, STRING, attrValue, op);
                break;
            case FLOAT: {
                *((float*) attrValue) = atof(attrValue);
                status = hfs.startScan(attrDesc.attrOffset, attrDesc.attrLen, FLOAT, attrValue, op);
                break;
            }
            case INTEGER: {
                *((int*) attrValue) = atoi(attrValue);
                status = hfs.startScan(attrDesc.attrOffset, attrDesc.attrLen, INTEGER, attrValue, op);
                break;
        }
	}

	// switch (attrDesc.attrType) {
    //         case STRING:
    //             status = hfs.startScan(attrDesc.attrOffset, attrDesc.attrLen, STRING, attrValue, op);
    //             break;
    //         case FLOAT: {
    //             float f = atof(attrValue);
    //             status = hfs.startScan(attrDesc.attrOffset, attrDesc.attrLen, FLOAT, (char *)&f, op);
    //             break;
    //         }
    //         case INTEGER: {
    //             int i = atoi(attrValue);
    //             status = hfs.startScan(attrDesc.attrOffset, attrDesc.attrLen, INTEGER, (char *)&i, op);
    //             break;
    //     }
	// }
	if (status != OK) return status;

	// Iterate through matching records and delete them
	while ((status = hfs.scanNext(rid)) == OK) {
		status = hfs.deleteRecord();
		if (status != OK) {
			hfs.endScan();
			return status;
		}
	}

	// End the scan and check termination status
	hfs.endScan();
	return (status == FILEEOF) ? OK : status;
}
