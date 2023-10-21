CC = gcc
CFLAGS = -Wall -Wextra -Wshadow -Wpointer-arith -std=c99 -pedantic -g -fsanitize=address
SRC = src/db.c
EXECUTABLE = db

all: $(EXECUTABLE)

$(EXECUTABLE): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(EXECUTABLE)

clean:
	rm -f $(EXECUTABLE)
