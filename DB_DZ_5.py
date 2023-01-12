import psycopg2


def create_tables():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS client(
        _id SERIAL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(70) NOT NULL UNIQUE
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phone(
        _id SERIAL PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES client(_id),
        number INTEGER NOT NULL UNIQUE
    );
    """)
    conn.commit()


def add_new_client(cursor, f_name: str, l_name: str, email: str) -> None:
    cursor.execute("""
        INSERT INTO client(first_name, last_name, email) VALUES(%s, %s, %s);
    """, (f_name, l_name, email))
    return conn.commit()


def add_phone(cursor, client_id: int, number: int) -> None:
    cursor.execute("""
        INSERT INTO phone(client_id, number) VALUES(%s, %s);
    """, (client_id, number))
    return conn.commit()


def edit_data(cursor, id: int, column: str, data, old_phone=None) -> None:
    if column == 'number':
        cursor.execute(
            f"UPDATE phone SET {column} = %s WHERE _id ="
            f"(SELECT _id FROM phone WHERE client_id = %s AND number = %s)",
            (data, id, old_phone)
        )
        conn.commit()
        return print(f'Изменен номер телефона у клиента id = {id}')
    cursor.execute(
        f"UPDATE client SET {column} = %s WHERE _id = %s;", (data, id)
    )
    conn.commit()
    return print(f'Изменены данные у клиента id = {id}')


def total_phone_numbers(cursor, id: int) -> int:
    cursor.execute("""
    SELECT COUNT(_id) FROM phone WHERE client_id = %s;
    """, (id,))
    return cur.fetchone()[0]


def del_phone_numbers(cursor, id: int) -> None:
    total = total_phone_numbers(cursor, id)
    for _ in range(total):
        cursor.execute("""
            DELETE FROM phone WHERE _id = 
            (SELECT _id FROM phone WHERE client_id = %s LIMIT 1);
        """, (id,))
        conn.commit()
    return print(f'Удалены все номера телефонов ({total}) клиента id = {id}')


def del_data_client(cursor, id: int) -> None:
    del_phone_numbers(cursor, id)
    cursor.execute("""
    DELETE FROM client WHERE _id = %s;
    """, (id,))
    conn.commit()
    return print(f'Удалены данные клиента с id = {id}')


def find_client(cursor, column: str, data) -> None:
    cursor.execute(
        f"SELECT * FROM client c "
        f"JOIN phone p ON c._id = p.client_id "
        f"WHERE {column} = %s", (data,)
    )
    return print(cursor.fetchall())


if __name__ == '__main__':
    with psycopg2.connect(database="DZ_5", user="postgres",password="317780") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE phone;
                DROP TABLE client;
                """)
            create_tables()
            add_new_client(cur, 'Andrew', 'Fadeew', 'asdf@jail.ge')
            add_new_client(cur, 'Petya', 'Vasilev', 'qwer@jail.de')
            add_phone(cur, 1, 12345)
            add_phone(cur, 1, 54321)
            add_phone(cur, 2, 55555)
            add_phone(cur, 2, 66666)
            edit_data(cur, 1, 'email', 'aaa@mumu.fe')
            edit_data(cur, 2, 'number', 99999, 66666)
            del_phone_numbers(cur, 1)
            del_data_client(cur, 1)
            find_client(cur, 'first_name', 'Petya')
            find_client(cur, 'number', 99999)
