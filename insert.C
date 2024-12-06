#include "catalog.h"
#include "query.h"

const Status QU_Insert(const string &relationName, 
        const int numAttributes, 
        const attrInfo attributeList[])
{
    Status operationStatus;
    int relationAttrCount;
    AttrDesc* relationAttributes;

    // Open an InsertFileScan for the target relation
    InsertFileScan insertScan(relationName, operationStatus);
    if (operationStatus != OK) { 
        return operationStatus; 
    }

    // Retrieve the schema of the target relation
    operationStatus = attrCat->getRelInfo(relationName, relationAttrCount, relationAttributes);
    if (operationStatus != OK) { 
        return operationStatus; 
    }

    // Calculate the total size of the record based on attribute lengths
    int recordSize = 0;
    for (int attrIdx = 0; attrIdx < relationAttrCount; attrIdx++) {
        recordSize += relationAttributes[attrIdx].attrLen;
    }

    // Allocate memory for the new record
    char recordData[recordSize];
    Record newRecord;
    newRecord.data = (void *)recordData;
    newRecord.length = recordSize;

    // Match and map attributes from input to the relation schema
    for (int inputAttrIdx = 0; inputAttrIdx < numAttributes; inputAttrIdx++) {
        for (int relAttrIdx = 0; relAttrIdx < relationAttrCount; relAttrIdx++) {
            if (strcmp(relationAttributes[relAttrIdx].attrName, attributeList[inputAttrIdx].attrName) == 0) {
                // Reject insertion if the attribute value is NULL
                if (attributeList[inputAttrIdx].attrValue == NULL) {
                    return ATTRTYPEMISMATCH;
                }

                char* convertedValue;
                int tempInt;
                float tempFloat;

                // Convert the attribute value to the correct data type
                switch (attributeList[inputAttrIdx].attrType) {
                    case INTEGER:
                        tempInt = atoi((char*)attributeList[inputAttrIdx].attrValue);
                        convertedValue = (char*)&tempInt;
                        break;
                    case FLOAT:
                        tempFloat = atof((char*)attributeList[inputAttrIdx].attrValue);
                        convertedValue = (char*)&tempFloat;
                        break;
                    case STRING:
                        convertedValue = (char*)attributeList[inputAttrIdx].attrValue;
                        break;
                }

                // Copy the converted value into the record buffer at the appropriate offset
                memcpy(recordData + relationAttributes[relAttrIdx].attrOffset, convertedValue, relationAttributes[relAttrIdx].attrLen);
            }
        }
    }

    // Insert the newly created record into the target relation
    RID recordID;
    operationStatus = insertScan.insertRecord(newRecord, recordID);
    if (operationStatus != OK) { 
        return operationStatus; 
    }

    // Return OK to indicate successful insertion
    return OK;
}
