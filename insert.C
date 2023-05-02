#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
Status ifs_status;
        InsertFileScan* ifs = new InsertFileScan(relation, ifs_status);
        if (ifs_status != OK) {
                return ifs_status;
        }

        AttrDesc *attrs;
        int getinfo_attrCnt;

        Status getinfo_status = attrCat -> getRelInfo(relation, getinfo_attrCnt, attrs);
        if (getinfo_status != OK) {
                return getinfo_status;
        }
        int tot_len = 0;
        //getting the length of the attributes
        for (int i=0; i<getinfo_attrCnt; i++) {
                tot_len += attrs[i].attrLen;
        }

        char out_attr[tot_len];
        Record rec;
        rec.data = (char *) out_attr;
        rec.length = tot_len;

        for (int i=0; i< attrCnt; i++) {
                //matching attributes
                for(int j=0; j<getinfo_attrCnt; j++) {
                        if(strcmp(attrList[i].attrName, attrs[j].attrName) == 0) {
                                if (attrList[i].attrValue == NULL) {
                                        return BADCATPARM;
                                }

                                char * cast_value;
                                int tint;
                                int fint;
                                switch (attrList[i].attrType) {
                                        case INTEGER:
                                                tint = atoi((char *)attrList[i].attrValue);
                                                cast_value = (char *)&tint;
                                                break;
                                        case FLOAT:
                                                fint = atof((char *)attrList[i].attrValue);
                                                cast_value = (char *)&fint;
                                                break;
                                        case STRING:
                                                cast_value = (char *)attrList[i].attrValue;
                                                break;

                                }
                                memcpy(out_attr + attrs[j].attrOffset, cast_value, attrs[j].attrLen);

                        }

                }


        }

        RID rid;
        Status insert_status = ifs->insertRecord(rec, rid);
        if (insert_status != OK) {
                        return insert_status;
        }

        return OK;

}

