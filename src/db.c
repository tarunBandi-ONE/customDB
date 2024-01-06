#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Constants for column sizes and offsets
#define COLUMN_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute) 
// 0 is the null pointer. This trick allows us to access some unspecified attribute

typedef enum{  META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND} MetaCommandResult;

typedef enum {PREPARE_SUCCESS,PREPARE_UNRECOGNIZED,PREPARE_SYNTAXERROR,PREPARE_STRING_TOO_LONG,PREPARE_ID_OOB} PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef enum {EXECUTE_SUCCESS, EXECUTE_TABLE_FULL} ExecuteResult; 

typedef enum{NODE_INTERNAL, NODE_LEAF} NodeType;



// Structure for a database row
typedef struct{
    uint32_t id;
    char user[COLUMN_SIZE+1];
    char email [COLUMN_EMAIL_SIZE+1];
}Row; 

// Structure for a SQL statement
typedef struct {
  StatementType type;
  Row row_toinsert;
} Statement; 

// Structure for input buffer, simply a string of characters
typedef struct{
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

//Constants for sizes and offsets
//if we have a block of memory i.e a byte array, offsets allow us to use pointer arithmetic
const uint32_t ID_SIZE = size_of_attribute(Row,id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, user);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USER_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_SIZE + USER_OFFSET;
const uint32_t ROWSIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; 
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100


const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = sizeof(uint8_t);
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_OFFSET;
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

uint32_t* leaf_node_num_cells(void* node){

    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num){

    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num)
{
    return leaf_node_cell(node,cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num)
{
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node)
{
    *leaf_node_num_cells(node) = 0;
}

//Struct for pager -> represents a file
typedef struct{
    int file_id;
    uint32_t fileLen;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct{
    uint32_t root_page_num;
    Pager* pager;
}Table; //Pages is an array of void pointers ~ arbitrary memory pointers which we use the above offsets that we calculated to figure out size

typedef struct{
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table; // Indicates when one past the last element
}Cursor;


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
        free(input_buffer->buffer);//string
        free(input_buffer);
    }   


//Input: Row, Output: Nothing, copies the memory to "destination"
void serialize_row(Row* source, void* destination)
{
    if (source == NULL)
    {
        printf("Row is null\n");
    }
    if (destination == NULL)
    {
        printf("Destination is null\n");
    }
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USER_OFFSET, &(source->user),USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email),EMAIL_SIZE);
}


//Input: Memory, Output: Copying it into a row.
void deserialize_row(void* source, Row* destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->user), source + USER_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

//Function that handles a cache miss.
void* get_page(Table* table, uint32_t page_num)
{
    if(page_num > TABLE_MAX_PAGES)
    {
        printf("Tried to print page that's out of bounds, %d > %d/n",page_num);
        exit(EXIT_FAILURE);
    }
    void* page = table->pager->pages[page_num];
    if (page == NULL)
    {
        //Cache miss:
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = table->pager->fileLen /PAGE_SIZE;
        
        //The length of the file is above the size of one page
        if (table->pager->fileLen % PAGE_SIZE!= 0)
        {
            num_pages +=1; 
        }
        
        //
        if(page_num <= num_pages)
        {
            //Move the cursor, then read PAGE_SIZE bytes
            lseek(table->pager->file_id,page_num * PAGE_SIZE,SEEK_SET);
            ssize_t numRead = read(table->pager->file_id,page,PAGE_SIZE);

            if (numRead == -1)
            {
                printf("Error reading the file");
                exit(EXIT_FAILURE);
            }
        }
        table->pager->pages[page_num] = page;
    }

    if (page_num >= pager->num_pages)
    {
        pager->num_pages = page_num + 1;
    }
    return table->pager->pages[page_num];
}

//Function which returns pointer to a specified row
void* cursor_value (Cursor* cursor)
{
    //Given row n, and m rows per page, the page it's on is n/m
    uint32_t pageNum = cursor->page_num; 
    void* page = get_page(cursor->table,pageNum);
    //There are ROWS_PER_PAGE rows per page, so using mod we figure out what specific row it is.
    uint32_t row_offset = cursor->row_num % ROWS_PER_PAGE;
    //specific row within the page * the bytes it takes for each row gives us the amt of bytes to offset
    return leaf_node_value(page, cursor->cell_num);

void cursor_advance(Cursor* cursor)
{
    cursor->cell_num +=1;
    void* node = get_page(cursor->table,cursor->page_num)
    if (cursor->cell_num >= (*leaf_node_num_cells(node)))
    {
        cursor ->end_of_table = true;
    }
}

Cursor* table_start(Table* table)
{
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor ->page_num = table -> root_page_num;
    cursor -> cell_num = 0;
    
    void* root_node = get_page(table, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor -> end_of_table = (num_cells == 0);
    return cursor;
}

Cursor* table_end(Table* table)
{
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor ->table = table;
    cursor ->page_num = table->root_page_num;

    void* root_node = get_page(table->pager, table -> root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor-> cell_num = num_cells;
    cursor->end_of_table = true;

    return cursor;
}

PrepareResult prepare_insert (Statement* s, InputBuffer* B) //Special logic for ensuring insertions are within bounds
{
    s->type = STATEMENT_INSERT;
    char* keywords = strtok(B->buffer," ");
    char* id = strtok(NULL, " ");//Strtok when given null continues to operate on the previous one, until returning NULL when theres no splits left
    char* user = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    if (id == NULL || user == NULL || email == NULL)
    {
        return PREPARE_SYNTAXERROR;
    }
    if (strlen(user) > USERNAME_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > EMAIL_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }
    int ids = atoi(id);
    if(ids <= 0)
    {
        return PREPARE_ID_OOB;
    }
    s->row_toinsert.id = ids;
    strcpy((s->row_toinsert.email),email);
    strcpy((s->row_toinsert.user),user);
    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement (Statement* s, InputBuffer* B) //load a statement using the input buffer
{
    if(strncmp(B->buffer,"insert",6)==0)
    {
        return prepare_insert(s,B);
    }else if (strncmp(B->buffer,"select",6)==0)
    {
        s->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS; 
    }
    return PREPARE_UNRECOGNIZED;
}



ExecuteResult execute_insert(Table* T, Statement* S) 
{
    if(T->num_rows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }
    Cursor* cursor = table_end(T);
    Row* r = &(S->row_toinsert);
    serialize_row(r,cursor_value(cursor)); //Copying from from the row, into the void* block 
    T->num_rows+=1;
    free(cursor);
    return EXECUTE_SUCCESS;

}

ExecuteResult execute_select(Table* T, Statement* S)
{
    Cursor *cursor = table_start(T);
    Row row;
    printf("%d\n",T->num_rows);

    while(!cursor->end_of_table)
    {
        deserialize_row(cursor_value(cursor),&row);
        printf("(%d, %s, %s)\n",row.id,row.email,row.user);
        cursor_advance(cursor);
    }

    free(cursor);
    return EXECUTE_SUCCESS;
}

Pager* pager_open(const char* file)
{

       int fileData = open(file,
     	  O_RDWR | 	// Read/Write mode
     	      O_CREAT,	// Create file if it does not exist
     	  S_IWUSR |	// User write permission
     	      S_IRUSR	// User read permission
     	  );

    if (fileData == -1)
    {
        perror("unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t file_len = lseek(fileData,0,SEEK_END);

    Pager* p = calloc(sizeof(Pager),1);
    p->file_id = fileData;
    p->fileLen = file_len;
    p->num_pages = (file_len/ PAGE_SIZE);

    if (file_len % PAGE_SIZE != 0)
    {
        printf("Not a whole number of pages");
        exit(EXIT_FAILURE);
    }
    for(int i =0; i < TABLE_MAX_PAGES; i++)
    {
        p->pages[i] = NULL;
    }
    return p;
}



Table* db_open(const char* file)

{
    Table* T = (Table*)malloc(sizeof(Table));
    Pager* P =  pager_open(file);
    uint32_t num_rows = P->fileLen / ROWSIZE;
    T->pager = P;
    T->num_rows = num_rows;
    return T;
}

void pager_flush(Pager* p, int page_num)
{
    if(p->pages[page_num] == NULL)
    {
        printf("trying to flush the null page\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(p->file_id, page_num*PAGE_SIZE, SEEK_SET);
    if (offset == -1)
    {
        printf("Error seeking/moving cursor\n");
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(p->file_id, p->pages[page_num], PAGE_SIZE);

    if (bytes_written != byte_num)
    {
        printf("Writing error\n");
        exit(EXIT_FAILURE);
    }
}
void db_close(Table* table)
{
    printf("Closing: \n");
    Pager* p = table->pager;
    printf("%d full\n",num_full);
    for(uint32_t i = 0; i < p->num_pages; i++)
    {
        printf("i: %d\n",i);
        void* page = p->pages[i];
        if(page != NULL)
        {
            pager_flush(p, i);
            free(page);
            p->pages[i] = NULL;
        }

    }


    int result = close(p->file_id);
    if(result == -1)
    {
        printf("Error closing file");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0 ; i < TABLE_MAX_PAGES; i++)
    {
        void* page = p->pages[i];
        if(page != NULL)
        {
            free(page);
            p->pages[i] = NULL;
        }
    }
    free(p);
    free(table);
}


ExecuteResult run_statement(Statement* s,Table* t)
{
    StatementType type = s->type;
    switch(type) {
        case(STATEMENT_INSERT):
            return execute_insert(t,s);
            break;
        case(STATEMENT_SELECT):
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
    //null term
    input_buffer->input_length = read - 1;
    input_buffer->buffer[read - 1] = 0;

}

MetaCommandResult do_command(InputBuffer* input_buffer, Table* t) //Running a meta command
{  
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    db_close(t);
    exit(EXIT_SUCCESS); //0 
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

int main(int argc, char* argv[])
{

    if(argc <2)
    {
        printf("Must supply a database name\n");
        exit(EXIT_FAILURE);
    }
    char* filename = argv[1];
    Table* T = db_open(filename);
    InputBuffer* input_buffer = new_input_buffer();
    while(true)
    {
        printf("db >>>");
        ReadInput(input_buffer);
        if(input_buffer->buffer[0]=='.')
        {
            switch (do_command(input_buffer,T))
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
            case(PREPARE_STRING_TOO_LONG):
                printf("Input string is too long\n");
                continue;
            case(PREPARE_ID_OOB):
                printf("ID Number not in bounds\n");
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
