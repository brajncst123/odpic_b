#include <stdlib.h>
#include <string.h>
#include <jansson.h>

#define MAXTABLES   100
#define MAXCOLUMNS  100

    struct schema_header
    {
        struct
        {
            char    type[50];
            char    user[128];
            char    password[256];
            char    connectionString[256];
        }connection_src;
        struct
        {
            char    type[50];
            char    host[128];
            int     port;
            char    database_name[100];
            char    user[128];
            char    password[256];
        }connection_dest;
    };

    struct schema_tables
    {
        char    tableName[128];
        char    schema[128];
        int     numberColumns;
        char    query[500];
        struct
        {
           char columnName[128];
           char type[50];
           char nullable[10];
        }column[MAXCOLUMNS];
    };




struct schema_header Migrator_Header( json_t * );
struct schema_tables *Migrator_Tables( json_t * );

