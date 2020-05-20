#include <dpi.h>
#include <stdlib.h>
#include <string.h>
#include "request-migrator.h"
#include "json-migrator.h"

#define URL_SIZE   256
#define URL_FORMAT "http://10.0.1.246:3000/migration/cc856a54-30c0-44c1-855d-b910360b4aa4/dataLoader"


static dpiContext *gContext = NULL;
static dpiErrorInfo gErrorInfo;


int printError(void)
{
    if (gContext)
        dpiContext_getError(gContext, &gErrorInfo);
    fprintf(stderr, " [FAILED]\n");
    fprintf(stderr, "    FN: %s\n", gErrorInfo.fnName);
    fprintf(stderr, "    ACTION: %s\n", gErrorInfo.action);
    fprintf(stderr, "    MSG: %.*s\n", gErrorInfo.messageLength,gErrorInfo.message);
    fflush(stderr);
    return DPI_FAILURE;
}



int
main(int argc, char *argv[])
{
    /*
    * ODPI DEFINITIONS
    */
    dpiData  *stringColValue1, *stringColValue2;
    dpiNativeTypeNum nativeTypeNum;
    uint32_t bufferRowIndex;
    dpiConn *conn;
    dpiStmt *stmt;
    int found;



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

    // create context
    if (dpiContext_create(DPI_MAJOR_VERSION, DPI_MINOR_VERSION, &gContext,
            &gErrorInfo) < 0)
        return printError();

    // create connection
    if (dpiConn_create(gContext, st_schema_header.connection_src.user, strlen(st_schema_header.connection_src.user),st_schema_header.connection_src.password , strlen(st_schema_header.connection_src.password),st_schema_header.connection_src.connectionString, strlen(st_schema_header.connection_src.connectionString), NULL, NULL, &conn) < 0)
        return printError();

    char selectSql[]="SELECT ";
    char from[]=" FROM ";
    char comma[]=",";
    char dot[]=".";

    for(int n=0; n<(sizeof(st_schema_tables)); n++)
    {
        printf("Table: %s.%s with columns %d \n",st_schema_tables[n].schema,st_schema_tables[n].tableName, st_schema_tables[n].numberColumns);
        for(int m=0; m<st_schema_tables[n].numberColumns; m++)
        {
            if(m==st_schema_tables[n].numberColumns-1)
                memmove(selectSql+strlen(selectSql), st_schema_tables[n].column[m].columnName, strlen(st_schema_tables[n].column[m].columnName)+1);
            else
            {
                printf("Table-Column: %s number column %d\n", st_schema_tables[n].column[m].columnName, m);
                memmove(selectSql+strlen(selectSql), st_schema_tables[n].column[m].columnName, strlen(st_schema_tables[n].column[m].columnName)+1);
                memmove(selectSql+strlen(selectSql), comma, strlen(comma)+1);
            }
        }
        printf("0-My Sql: %s \n", selectSql);
        memmove(selectSql+strlen(selectSql), from, strlen(from)+1);
        memmove(selectSql+strlen(selectSql), st_schema_tables[n].schema, strlen(st_schema_tables[n].schema)+1);
        memmove(selectSql+strlen(selectSql), dot, strlen(dot)+1);
        memmove(selectSql+strlen(selectSql), st_schema_tables[n].tableName, strlen(st_schema_tables[n].tableName)+1);
        printf("1-My Sql: %s \n", selectSql);
        break;
    }

    if (dpiConn_prepareStmt(conn, 0, selectSql, strlen(selectSql), NULL, 0,
            &stmt) < 0)
        return printError();

    if (dpiStmt_execute(stmt, DPI_MODE_EXEC_DEFAULT, NULL) < 0)
        return printError();

    printf("My Sql: %s \n", selectSql);
     while (1) 
    {
        if (dpiStmt_fetch(stmt, &found, &bufferRowIndex) < 0)
            return printError();
        if (!found)
            break;
        if (dpiStmt_getQueryValue(stmt, 1, &nativeTypeNum, &stringColValue1) < 0)
            return printError();
        printf("Id = '%.*s' String = '%.*s'\n",
        stringColValue1->value.asBytes.length,
        stringColValue1->value.asBytes.ptr);

        if (dpiStmt_getQueryValue(stmt, 2, &nativeTypeNum, &stringColValue1) < 0)
            return printError();
        printf("Id = '%.*s' String = '%.*s'\n",
        stringColValue1->value.asBytes.length,
        stringColValue1->value.asBytes.ptr);
    }



    dpiStmt_release(stmt);
    dpiConn_release(conn);


/*
    printf("Header!: %s\n", st_schema_header.connection_src.connectionString);

    for(int n=0; n<(sizeof(st_schema_tables)); n++)
    {
        printf("Table: %s.%s \n",st_schema_tables[n].schema,st_schema_tables[n].tableName);
        for(int m=0; m<st_schema_tables[n].numberColumns; m++)
        {
            printf("Table-Column: %s\n", st_schema_tables[n].column[m].columnName);
        }
    }
*/

    return 0;
}
