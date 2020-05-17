#include "json-migrator.h"

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
    size_t          i,i2;
    json_t          *in;

    in = json_object_get(par, "tables");

    if (!json_is_array(in))
    {
        fprintf(stderr, "error: in is not an array\n");
    }

    json_t  *data, *intable, *column;
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
        
        column = json_object_get(data, "columns" );
        if (json_is_array(column))
        {
            tables[i].numberColumns = (int) json_array_size(column);
            for (i2 = 0; i2 < json_array_size(column); i2++)
            {
                json_t  *field;
                field  = json_array_get(column, i2);
                 if (!json_is_object(field))
                fprintf(stderr, "error: commit data %d is not an object\n", (int)(i + 1));
                strcpy(tables[i].column[i2].columnName,json_string_value( json_object_get(field, "columnName" )));
                strcpy(tables[i].column[i2].type, json_string_value( json_object_get(field, "type")));
                //strcpy(tables[i].column[i2].nullable,json_string_value(fieldProperty));
            }
        }
    }
    return tables;

}
