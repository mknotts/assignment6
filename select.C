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
    Status status = OK; 
    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            return status;
        }
    }
    AttrDesc attrDesc;
    if (attr != NULL){
        status = attrCat->getInfo(attr->relName,
                                     attr->attrName,
                                     attrDesc);
        if (status != OK)
        {
            return status;
        }
    }
    // get output record length from attrdesc structures
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }

    if (attr != NULL){
        ScanSelect(result, projCnt, attrDescArray, &attrDesc, op, attrValue, reclen);
    } else {
        ScanSelect(result, projCnt, attrDescArray, NULL, op, attrValue, reclen);
    }
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
    // scan table
    RID selectRID;
    Record selectRec;
    
    Operator myop;
    switch(op) {
      case EQ:   myop=EQ; break;
      case GT:   myop=GT; break;
      case GTE:  myop=GTE; break;
      case LT:   myop=LT; break;
      case LTE:  myop=LTE; break;
      case NE:   myop=NE; break;
    }
    // start scan on table

    string filename = "";
    if (attrDesc == NULL){
        filename = string(projNames[0].relName);
    } else {
        filename = attrDesc->relName;
    }
    HeapFileScan selectScan(filename, status);
    if (status != OK) { return status; }
    if (attrDesc == NULL){
        status = selectScan.startScan(0,
                                    0,
                                    STRING,
                                    NULL,
                                    EQ);
        if (status != OK) { return status; }
    } else {
        int tint;
        float fint;
        switch((Datatype) attrDesc->attrType){
            case INTEGER:
                cout << "is integer" << endl;
                tint = atoi(filter);
                cout << "tint: " << tint << endl;
                filter = (char*)(&tint);
                cout << "filter: " << atoi(filter) << endl;
                break;
            case FLOAT:
                cout << "is float" << endl;
                fint = atof(filter);
                cout << "fint: " << fint << endl;
                filter = (char*)(&fint);
                cout << "filter: " << atof(filter) << endl;
                break;
            case STRING:
                cout << "is string" << endl;
                break;
        }
        status = selectScan.startScan(attrDesc->attrOffset,
                                    attrDesc->attrLen,
                                    (Datatype) attrDesc->attrType,
                                    filter,
                                    myop);
        if (status != OK) { return status; }
    }

    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;
    while (selectScan.scanNext(selectRID) == OK)
    {        
		status = selectScan.getRecord(selectRec);
		if (status != OK) { return status; }
		// we have a match, copy data into the output record
		int outputOffset = 0;
		for (int i = 0; i < projCnt; i++)
		{
			// copy the data out of the proper input file (inner vs. outer)
            memcpy(outputData + outputOffset,
                    (char *)selectRec.data + projNames[i].attrOffset,
                    projNames[i].attrLen);
			outputOffset += projNames[i].attrLen;
		} // end copy attrs

		// add the new record to the output relation
		RID outRID;
		status = resultRel.insertRecord(outputRec, outRID);
		if (status != OK) { return status; }
		resultTupCnt++;
    } // end scan 
    return status;

}
