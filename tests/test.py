import subprocess

def run_script(commands):
    raw_output = None
    with subprocess.Popen(["../db","test.db"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as process:
        for command in commands:
            process.stdin.write(command + "\n")
        process.stdin.close()
        raw_output = process.stdout.read()

    return raw_output.split("\n")

def test_insert_and_retrieve_row():
    result = run_script([
        "insert 1 user1 person1@example.com",
        "select",
        ".exit",
    ])
    
    expected_output = [
        "db > Executed.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db >> ",
    ]
    
    print(result)

def test_table_error_message():
    script = [f"insert {i} user{i} person{i}@example.com" for i in range (1, 1402)]
    print(script[0])
    script.append(".exit")

    result = run_script(script)

    print(result[-2])


def test_inserting_maximum_length_strings():
    long_username = "a" * 10
    long_email = "a" * 1
    
    script = [
        f"insert -213 {long_username} {long_email}",
        "select",
        ".exit",
    ]
    result = run_script(script)
    print(result)

def test_printing_strings():
    script = [
        "insert 1 user1 person1@example.com",
        "select",
        ".constants",
        ".btree"
    ]
    print(run_script(script))


if __name__ == "__main__":
    test_insert_and_retrieve_row()
    test_table_error_message()
    test_inserting_maximum_length_strings()
