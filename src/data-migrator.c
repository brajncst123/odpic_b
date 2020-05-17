#include <stdlib.h>
#include <string.h>
#include "request-migrator.h"
#include "json-migrator.h"

#define URL_SIZE   256
#define URL_FORMAT "http://10.0.1.246:3000/migration/cc856a54-30c0-44c1-855d-b910360b4aa4/dataLoader"

int
main(int argc, char *argv[])
{
    char   *text;
    char    url[URL_SIZE];
    json_t  *root;
    json_error_t    error;

    if (argc != 2 )
    {
        fprintf(stderr, "usage: %s URL\n\n", argv[0]);
        return 2;
    }

    //strcpy(url,argv[1]);
    strcpy(url, URL_FORMAT);

    printf("URL: %s\n", url);

    text = request(url); //Here we get the object as a char
    if (!text)
        return 1;

    root = json_loads( text , 0, &error );
    free(text);

    if (!root)
    {
        fprintf(stderr, "error: on line %d: %s\n", error.line, error.text);
        return 1;
    }

    if (!json_is_object(root))
    {
        fprintf(stderr, "error: root is not an object\n");
        json_decref(root);
        return 1;
    }

    struct schema_header st_schema_header = Migrator_Header( root  );
    struct schema_tables *st_schema_tables;

    st_schema_tables = Migrator_Tables( root );

    /*
    *   From here we'll write migration code
    *   ODPIC & libpq
    */

    printf("Header!: %s\n", st_schema_header.connection_src.connectionString);

    for(int n=0; n<(sizeof(st_schema_tables)); n++)
    {
        printf("Table: %s \n", st_schema_tables[n].tableName);
        for(int m=0; m<st_schema_tables[n].numberColumns; m++)
        {
            printf("Table-Column: %s\n", st_schema_tables[n].column[m].columnName);
        }
    }
    return 0;
}
