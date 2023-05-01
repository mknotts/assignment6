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
	if (relation.empty())
    	return BADCATPARM;

	Status status;
	RID rid;

	HeapFileScan* hfs;
	hfs = new HeapFileScan(relation, status);
	if (status != OK) return status;

	AttrDesc *ad;

	if (attrName == "") {
		status = hfs->startScan(0, 0, type, NULL, op);
		if (status != OK) {
			return status;
		}
	} else {
		status = attrCat->getInfo(relation, attrName, &ad);
		if (status != OK) {
			return status;
		}

		status = hfs->startScan(ad->attrOffset, ad->attrLen, ad->attrType, relation.c_str(), op);
	}

	while((hfs->scanNext(rid)) == OK) {
		hfs->deleteRecord();
	}

	hfs->endScan();

	delete hfs;
	
	// part 6
	return OK;
}


