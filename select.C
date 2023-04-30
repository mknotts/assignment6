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
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
    Status status; 

    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            return status;
        }
    }

    AttrDesc attrDesc;
    status = attrCat->getInfo(attr->relName,
                                     attr->attrName,
                                     attrDesc);
    if (status != OK)
    {
        return status;
    }

    // get output record length from attrdesc structures
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }

    ScanSelect(result, projCnt, attrDescArray, &attrDesc, op, attrValue, reclen);

    return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status = OK;
    int resultTupCnt = 0;
	// open the result table
    InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }

    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;
    
    // scan table
    RID selectRID;
    Record selectRec;
    
    Operator myop;
    switch(op) {
      case EQ:   myop=EQ; break;
      case GT:   myop=LT; break;
      case GTE:  myop=LTE; break;
      case LT:   myop=GT; break;
      case LTE:  myop=GTE; break;
      case NE:   myop=NE; break;
    }

    // start scan on table
    HeapFileScan selectScan(string((*attrDesc).relName), status);
    if (status != OK) { return status; }
    status = selectScan.startScan(attrDesc->attrOffset,
                                 attrDesc->attrLen,
                                 (Datatype) attrDesc->attrType,
                                 filter,
                                 myop);
    if (status != OK) { return status; }

    while (selectScan.scanNext(selectRID) == OK)
    {
		status = selectScan.getRecord(selectRec);
		ASSERT(status == OK);
		// we have a match, copy data into the output record
		int outputOffset = 0;
		for (int i = 0; i < projCnt; i++)
		{
			// copy the data out of the proper input file (inner vs. outer)
			if (0 == strcmp(projNames[i].relName, attrDesc->relName))
			{
				memcpy(outputData + outputOffset,
						(char *)selectRec.data + projNames[i].attrOffset,
						projNames[i].attrLen);
			}
			outputOffset += projNames[i].attrLen;
		} // end copy attrs

		// add the new record to the output relation
		RID outRID;
		status = resultRel.insertRecord(outputRec, outRID);
		ASSERT(status == OK);
		resultTupCnt++;
    } // end scan 
    return status;

}
