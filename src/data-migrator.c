#include <dpi.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "request-migrator.h"
#include "json-migrator.h"

#define URL_SIZE   256
#define URL_FORMAT "http://10.0.1.246:3000/migration/86b78238-5861-48f4-af00-4dd110bacb7f/dataLoader"


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
    dpiData *Value, *refCursorValue; 
    dpiNativeTypeNum nativeTypeNum;
    uint32_t bufferRowIndex, numQueryColumns;
    dpiConn *conn;
    dpiStmt *stmt;
    dpiQueryInfo info;
    dpiVar *refCursorVar;
    int found;

    dpiTimestamp *timestamp;

    char   *text;
    char    url[URL_SIZE];
    json_t  *root;
    json_error_t    error;

    if (argc != 2 )
    {
        fprintf(stderr, "usage: %s URL\n\n", argv[0]);
        return 2;
    }

    //strcpy(url,argv[1]); In the near future the argument will pass from ??
    strcpy(url, URL_FORMAT);

    //printf("URL: %s\n", url);

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

    // create context
    if (dpiContext_create(DPI_MAJOR_VERSION, DPI_MINOR_VERSION, &gContext,
            &gErrorInfo) < 0)
        return printError();

    // create connection
    if (dpiConn_create(gContext, st_schema_header.connection_src.user, strlen(st_schema_header.connection_src.user),st_schema_header.connection_src.password , strlen(st_schema_header.connection_src.password),st_schema_header.connection_src.connectionString, strlen(st_schema_header.connection_src.connectionString), NULL, NULL, &conn) < 0)
        return printError();

    char selectSql[]="BEGIN OPEN :cursor FOR SELECT ";
    char from[]=" FROM ";
    char comma[]=",";
    char dot[]=".";
    char delimiter[]="\t";
    char endSelect[]="; END;";

    for(int n=0; n<(sizeof(st_schema_tables)-1); n++)
    {
        printf("QUERY: %s", st_schema_tables[n].query);
        for(int m=1; m<=st_schema_tables[n].numberColumns; m++)
        {
            if(m==st_schema_tables[n].numberColumns)
                memmove(selectSql+strlen(selectSql), st_schema_tables[n].column[m].columnName, strlen(st_schema_tables[n].column[m].columnName)+1);
            else
            {
                memmove(selectSql+strlen(selectSql), st_schema_tables[n].column[m].columnName, strlen(st_schema_tables[n].column[m].columnName)+1);
                memmove(selectSql+strlen(selectSql), comma, strlen(comma)+1);
            }
        }
        memmove(selectSql+strlen(selectSql), from, strlen(from)+1);
        memmove(selectSql+strlen(selectSql), st_schema_tables[n].schema, strlen(st_schema_tables[n].schema)+1);
        memmove(selectSql+strlen(selectSql), dot, strlen(dot)+1);
        memmove(selectSql+strlen(selectSql), st_schema_tables[n].tableName, strlen(st_schema_tables[n].tableName)+1);
        memmove(selectSql+strlen(selectSql), endSelect , strlen(endSelect)+1);

        //break;
    //}

/*
    for(int n=0; n<(sizeof(st_schema_tables)-1); n++)
    {
        printf("nro %d Query: %s \n",n, st_schema_tables[n].query);
    }

*/

    //char delimiter[]="\t";
    //char selectSql[] = "begin open :cursor for select \"REGION_ID\",\"COUNTRY_NAME\",\"COUNTRY_ID\" from \"HR\".\"COUNTRIES\"; end;";
    

    printf(" Pre-Execute My Sql: %s \n",  selectSql);
    if (dpiConn_prepareStmt(conn, 0, selectSql, strlen(selectSql), NULL, 0,
            &stmt) < 0)
        return printError();
    if (dpiConn_newVar(conn, DPI_ORACLE_TYPE_STMT, DPI_NATIVE_TYPE_STMT, 1, 0, 0, 0, NULL, &refCursorVar, &refCursorValue) < 0)
        return printError();
    
    if (dpiStmt_bindByPos(stmt, 1, refCursorVar) < 0)
        return printError();

    if (dpiStmt_execute(stmt, 0, &numQueryColumns) < 0)
        return printError();

    dpiStmt_release(stmt);
    stmt = refCursorValue->value.asStmt;

    int NumberOfColumns=0;
    while (1)
    {
        if (dpiStmt_fetch(stmt, &found, &bufferRowIndex) < 0)
            return printError();
        if (!found)
            break;

        if (dpiStmt_getNumQueryColumns(stmt, &numQueryColumns) < 0)
            return printError();

        char *record = (char *) calloc(100, sizeof( char * ));
        char *f;
        for(int col=1; col<=st_schema_tables[n].numberColumns; col++)
        {
            if(dpiStmt_getQueryInfo( stmt, col, &info)<0)
                return printError();

            if (dpiStmt_getQueryValue(stmt, col, &nativeTypeNum, &Value) < 0)
                return printError();

            int precision, digit;
            uint8_t scale;
            switch(info.typeInfo.oracleTypeNum)
            {
                case DPI_ORACLE_TYPE_VARCHAR :
                    f = calloc(Value->value.asBytes.length, sizeof( char *));
                    sprintf(f, "%.*s", Value->value.asBytes.length, Value->value.asBytes.ptr);
                    break;
                case DPI_ORACLE_TYPE_CHAR :
                    f = calloc(Value->value.asBytes.length, sizeof( char *));
                    sprintf(f, "%.*s", Value->value.asBytes.length, Value->value.asBytes.ptr);
                    break;
                case DPI_ORACLE_TYPE_NUMBER :
                    precision= (info.typeInfo.precision==0)?38:info.typeInfo.precision;
                    scale = info.typeInfo.scale; //TO DO:  take a count negative scale!!
                    digit = precision + scale;
                    f = calloc(digit, sizeof(int));
                    if(scale != 0)
                        sprintf(f, "%f", Value->value.asDouble);
                    else
                        sprintf(f, "%ld", Value->value.asInt64);
                    break;
                case DPI_ORACLE_TYPE_DATE :
                    timestamp = &Value->value.asTimestamp;
                    f = calloc(30, sizeof( char *));
                    sprintf(f, "%4d-%.2d-%.2d %.2d:%.2d:%.2d.%.6d",
                        timestamp->year, timestamp->month, timestamp->day,
                        timestamp->hour, timestamp->minute, timestamp->second,
                        timestamp->fsecond);
                    break;
                case DPI_ORACLE_TYPE_TIMESTAMP :
                    strcpy(f,"*Col TIMESTAMP*");
                    break;
                case DPI_ORACLE_TYPE_TIMESTAMP_TZ :
                    strcpy(f,"*Col TIMESTAMP TZ*");
                    break;
                case DPI_ORACLE_TYPE_TIMESTAMP_LTZ :
                    strcpy(f,"*Col TIMESTAMP LTZ*");
                    break;
                default :
                    strcpy(f, "*Col isnt recognized*");
                    break;
            }
            
            memmove(record+strlen(record), f,strlen(f)+1);
            if(col!=NumberOfColumns)
                memmove(record+strlen(record), delimiter, strlen(delimiter)+1);
        }
        printf("%s\n", record);
        free(record);
    }
    
    dpiVar_release(refCursorVar);
    memcpy(selectSql, "BEGIN OPEN :cursor FOR SELECT ", 31);
    }
    dpiConn_release(conn);
    dpiContext_destroy(gContext);
    
   

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
