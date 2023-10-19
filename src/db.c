#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define COLUMN_SIZE 32

#define COLUMN_EMAIL_SIZE 255

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)




typedef enum{  META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {PREPARE_SUCCESS,PREPARE_UNRECOGNIZED,PREPARE_SYNTAXERROR} PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef enum {EXECUTE_SUCCESS, EXECUTE_TABLE_FULL} ExecuteResult;

typedef struct{
    uint32_t id;
    char user[COLUMN_SIZE];
    char email [COLUMN_EMAIL_SIZE];
}Row;

typedef struct {
  StatementType type;
  Row row_toinsert;
} Statement;


typedef struct{
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;


const uint32_t ID_SIZE = size_of_attribute(Row,id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, user);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USER_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_SIZE + USER_OFFSET;
const uint32_t ROWSIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROWSIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct{
    uint32_t num_rows;
    void* pages[TABLE_MAX_ROWS];
}Table;

InputBuffer* new_input_buffer()
{
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer -> buffer = NULL;
    input_buffer ->buffer_length = 0;
    input_buffer ->input_length = 0;
    return input_buffer;
}

void close_input_buffer(InputBuffer *input_buffer)
    {
        free(input_buffer->buffer);
        free(input_buffer);
    }   

void serialize_row(Row* source, void* destination)
{
    if (source == NULL)
    {
        printf("Row is null\n");
    }
    if (destination == NULL) //>insert 1 tbandi t
    {
        printf("Destination is null\n");
    }
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USER_OFFSET, &(source->user),USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email),EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->user), source + USER_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* row_slot (Table* table, uint32_t rownum)
{
    uint32_t pageNum = rownum/ROWS_PER_PAGE;
    void* page = table->pages[pageNum];
    if (page == NULL)
    {
        table->pages[pageNum] = malloc(PAGE_SIZE);
        page = table->pages[pageNum];
    }
    uint32_t row_offset = rownum % ROWS_PER_PAGE;
    uint32_t byteOffset = row_offset * ROWSIZE;
    if (page + byteOffset == NULL)
    {
        printf("byteoffset: %d\n",byteOffset);
    }
    return page + byteOffset;
}


PrepareResult prepare_statement (Statement* s, InputBuffer* B)
{
    if(strncmp(B->buffer,"insert",6)==0)
    {
        s->type = STATEMENT_INSERT;
        int args = sscanf(B->buffer,"insert %d %s %s",&(s->row_toinsert.id),
                         (s->row_toinsert.email),(s->row_toinsert.user));
        if (args < 3)
        {
            return PREPARE_SYNTAXERROR;
        }
        return PREPARE_SUCCESS;
    }else if (strncmp(B->buffer,"select",6)==0)
    {
        s->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS; 
    }
    return PREPARE_UNRECOGNIZED;
}

MetaCommandResult do_command(InputBuffer* input_buffer)
{  
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

ExecuteResult execute_insert(Table* T, Statement* S)
{
    if(T->num_rows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }
    Row* r = &(S->row_toinsert);
    serialize_row(r,row_slot(T,T->num_rows));
    T->num_rows+=1;
    return EXECUTE_SUCCESS;

}

ExecuteResult execute_select(Table* T, Statement* S)
{
    Row row;
    for(int i=0;i<T->num_rows;i++)
    {
        deserialize_row(row_slot(T,i),&row);
        printf("(%d, %s, %s)\n",row.id,row.email,row.user);
    }
    return EXECUTE_SUCCESS;
}

Table* new_table()
{
    Table* T = (Table*)malloc(sizeof(Table));
    T->num_rows = 0;
    for(int i =0; i < TABLE_MAX_ROWS; i++)
    {
        T->pages[i] = NULL;
    }
    return T;
}

void free_table(Table* T)
{
    for(int i =0; i < T->num_rows; i++)
    {
        free(T->pages[i]);
    }
    free(T);
}

ExecuteResult run_statement(Statement* s,Table* t)
{
    StatementType type = s->type;
    switch(type) {
        case(STATEMENT_INSERT):
            printf("inserting\n");
            return execute_insert(t,s);
            break;
        case(STATEMENT_SELECT):
            printf("selecting\n");
            return execute_select(t,s);
            break;
    }
}
void ReadInput(InputBuffer* input_buffer)
{
    ssize_t read = getline(&(input_buffer->buffer),&(input_buffer->buffer_length),stdin);
    if(read <= 0)
    {
        printf("error reading input");
        close_input_buffer(input_buffer);
        exit(EXIT_FAILURE);
    }
    //new line
    input_buffer->input_length = read - 1;
    input_buffer->buffer[read - 1] = 0;

}


int main(int argc, char* argv[])
{
    Table* T= new_table();
    InputBuffer* input_buffer = new_input_buffer();
    while(true)
    {
        printf("db >>");
        ReadInput(input_buffer);
        if(input_buffer->buffer[0]=='.')
        {
            switch (do_command(input_buffer))
            {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch(prepare_statement(&statement,input_buffer))
        {
            case(PREPARE_SUCCESS):
                break;
            case(PREPARE_UNRECOGNIZED):
                printf("Unrecognized keyword '%s'\n",input_buffer->buffer);
                continue;
            case(PREPARE_SYNTAXERROR):
                printf("Syntax Error\n");
                continue;
        }
        switch(run_statement(&statement,T))
        {
            case (EXECUTE_SUCCESS):
                printf("Success\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error, table is full.\n");
                break; 
        }
    }
}
