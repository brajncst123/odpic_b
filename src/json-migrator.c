#include "json-migrator.h"

char *zStrrep(char *str, char x, char y)
{
    char *tmp=str;
    while(*tmp)
        if(*tmp == x)
            *tmp++ = y; /* assign first, then incement */
        else
            *tmp++;

    *tmp='\0';
    return str;
}

char *replace(const char*instring, const char *old_part, const char *new_part)
{

#ifndef EXPECTED_REPLACEMENTS
    #define EXPECTED_REPLACEMENTS 100
#endif

    if(!instring || !old_part || !new_part)
    {
        return (char*)NULL;
    }

    size_t instring_len=strlen(instring);
    size_t new_len=strlen(new_part);
    size_t old_len=strlen(old_part);
    if(instring_len<old_len || old_len==0)
    {
        return (char*)NULL;
    }

    const char *in=instring;
    const char *found=NULL;
    size_t count=0;
    size_t out=0;
    size_t ax=0;
    char *outstring=NULL;

    if(new_len> old_len )
    {
        size_t Diff=EXPECTED_REPLACEMENTS*(new_len-old_len);
        size_t outstring_len=instring_len + Diff;
        outstring =(char*) malloc(outstring_len); 
        if(!outstring){
            return (char*)NULL;
        }
        while((found = strstr(in, old_part))!=NULL)
        {
            if(count==EXPECTED_REPLACEMENTS)
            {
                outstring_len+=Diff;
                if((outstring=realloc(outstring,outstring_len))==NULL)
                {
                     return (char*)NULL;
                }
                count=0;
            }
            ax=found-in;
            strncpy(outstring+out,in,ax);
            out+=ax;
            strncpy(outstring+out,new_part,new_len);
            out+=new_len;
            in=found+old_len;
            count++;
        }
    }
    else
    {
        outstring =(char*) malloc(instring_len);
        if(!outstring){
            return (char*)NULL;
        }
        while((found = strstr(in, old_part))!=NULL)
        {
            ax=found-in;
            strncpy(outstring+out,in,ax);
            out+=ax;
            strncpy(outstring+out,new_part,new_len);
            out+=new_len;
            in=found+old_len;
        }
    }
    ax=(instring+instring_len)-in;
    strncpy(outstring+out,in,ax);
    out+=ax;
    outstring[out]='\0';

    return outstring;
}

struct schema_header Migrator_Header( json_t *par )
{

    struct schema_header sh;

    const char     *key, *key2;

    json_t          *src, *dest, *obj, *obj2;

    src  = json_object_get(par, "src");
    if(!json_is_object(src))
        printf("src it's not an object \n");

    json_object_foreach(src, key, obj)
    {
        if(strcmp(key, "type")==0)
        {
            strcpy(sh.connection_src.type, json_string_value(obj));
        }
        if(json_is_object(obj))
        {
            json_object_foreach(obj, key2, obj2)
            {
                if(strcmp(key2, "user") == 0 )
                    strcpy(sh.connection_src.user, json_string_value(obj2));
                else if(strcmp(key2, "password") == 0)
                    strcpy(sh.connection_src.password, json_string_value(obj2));
                else if(strcmp(key2, "connectionString") == 0)
                    strcpy(sh.connection_src.connectionString, json_string_value(obj2));
            }
        }
    }

    dest = json_object_get(par, "dest");
    if(!json_is_object(dest))
        printf("src it's not an object \n");

    json_object_foreach(dest, key, obj)
    {
        if(strcmp(key, "type")==0)
            strcpy(sh.connection_dest.type, json_string_value(obj));
        
        if(json_is_object(obj))
        {
            json_object_foreach(obj, key2, obj2)
            {
                if(strcmp(key2, "host") == 0)
                    strcpy(sh.connection_dest.host, json_string_value(obj2));
                else if(strcmp(key2, "port") == 0)
                    //strcpy(sh.connection_dest.port, json_integer_value(obj2));
                    sh.connection_dest.port=json_integer_value(obj2);
                else if(strcmp(key2, "database") == 0)
                    strcpy(sh.connection_dest.database_name, json_string_value(obj2));
                else if(strcmp(key2, "user") == 0)
                    strcpy(sh.connection_dest.user, json_string_value(obj2));
                else if(strcmp(key2, "password") == 0 )
                    strcpy(sh.connection_dest.password, json_string_value(obj2));
            }
        }
    }
    return sh;

}

struct schema_tables tables[MAXTABLES];

struct schema_tables *Migrator_Tables( json_t *par )
{
    const char     *key;
    size_t          i,i2;
    json_t          *in;

    in = json_object_get(par, "tables");

    if (!json_is_array(in))
    {
        fprintf(stderr, "error: in is not an array\n");
    }

    json_t  *data, *intable, *sch, *obj, *column, *query;

    for (i = 0; i < json_array_size(in); i++)
    {
        data = json_array_get(in, i);
        if (!json_is_object(data))
        {
            fprintf(stderr, "error: commit data %d is not an object\n", (int)(i + 1));
        }
        intable =  json_object_get(data, "tableName" );
        if(json_is_string(intable))
            strcpy(tables[i].tableName,json_string_value(intable));

        sch  = json_object_get(data, "schema");
        if(!json_is_object(sch))
            printf("src it's not an object \n");
        json_object_foreach(sch, key, obj)
        {
            if(strcmp(key, "schema")==0)
                strcpy(tables[i].schema, json_string_value(obj));
        }
        query = json_object_get(data, "query" );
        //if(json_is_string(query))
        //    strcpy(tables[i].query,json_string_value(query));

        //zStrrep(char *str, char x, char y)
        char x[]="\"";
        char y[]="\\\"";
        strcpy(
            tables[i].query,
            replace(
                    json_string_value(query),
                    x,
                    y
                    )
        );


        column = json_object_get(data, "columns" );
        if (json_is_array(column))
        {
            tables[i].numberColumns = (int) json_array_size(column);
            for (i2 = 0; i2 < json_array_size(column); i2++)
            {
                json_t  *field;
                int position;
                field  = json_array_get(column, i2);
                if (!json_is_object(field))
                    fprintf(stderr, "error: commit data %d is not an object\n", (int)(i + 1));
                position = json_integer_value( json_object_get( field, "position"));
                strcpy(tables[i].column[position].columnName,json_string_value( json_object_get(field, "columnName" )));
                strcpy(tables[i].column[position].type, json_string_value( json_object_get(field, "type")));
                //strcpy(tables[i].column[i2].nullable,json_string_value(fieldProperty));
            }
        }
    }
    return tables;

}
