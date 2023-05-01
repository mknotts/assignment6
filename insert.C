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



AttrDesc *attrs;
        int getinfo_attrCnt;

        Status status = attrCat -> getRelInfo(relation, getinfo_attrCnt, attrs);
        if (status != OK) {
                return status;
        }
        int tot_len = 0;
        //getting the length of the attributes
        for (int i=0; i<getinfo_attrCnt, i++) {
                total_len += attrs.attrLen;
        }

        char out_attr[total_len];
        Record rec;
        rec.data = (char *) out_attr;
        rec.length = total_len;



        for (int i=0; i< attrCnt; i++) {
                //matching attributes
                for(int j=0; j<getinfo_attrCnt; j++) {
                        if(strcmp(attrList[i].attrName, attrs[j].attrName) == 0) {
                                if (attrList[i].attrValue == NULL) {
                                        return NULL;
                                }

                                char * cast_value;
                                if (attrList[i] == INTEGER) {
                                        cast_value = (char*)(atoi(attrList[i].attrValue));
                                }
                                if(attrList[i] == FLOAT){
                                        cast_value = (char*)(atof(attrList[i].attrValue));
                                }
                                if(attrList[i] == STRING) {
                                        cast_value == (char*)attrList[i].attrValue;
                                }

                        }
                        //memcpy
                        memcpy(out_attr + attrs[j].attrOffset, cats_value, attrs[j].attrLen);

                }

        }






return OK;

}

