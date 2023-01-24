def create_tables(conn):
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        /*DROP TABLE fct_positions;*/
        CREATE TABLE IF NOT EXISTS fct_positions (
            day_id INTEGER NOT NULL,
            pos_id VARCHAR(255) NOT NULL,
            pos_name VARCHAR(255) NOT NULL,
            current_price NUMERIC NOT NULL,
            quantity NUMERIC NOT NULL,
            currency VARCHAR(255) NOT NULL,
            value NUMERIC NOT NULL,
            instrument_type VARCHAR(255) NOT NULL,
            fx_eur NUMERIC NOT NULL,
            value_eur NUMERIC NOT NULL,
            inserted TIMESTAMP NOT NULL,
            PRIMARY KEY (day_id , pos_id)
        )
        """,
        """
        /*DROP TABLE fct_transactions;*/
        CREATE TABLE IF NOT EXISTS fct_transactions (  
            day_id INTEGER NOT NULL,     
            trx_id INTEGER NOT NULL, 
            pos_id VARCHAR(255) NOT NULL,
            description VARCHAR(255) NOT NULL,
            trx_type VARCHAR(255) NOT NULL,           
            quantity NUMERIC NOT NULL,
            price NUMERIC NOT NULL,           
            value NUMERIC NOT NULL,
            currency VARCHAR(255) NOT NULL,  
            inserted TIMESTAMP NOT NULL,         
            PRIMARY KEY (day_id, trx_id, pos_id)
        )
        """)

    # open PostgreSQL cursor
    cur = conn.cursor()

    # create table one by one
    for command in commands:
        cur.execute(command)

    # commit the changes
    conn.commit()


# if __name__ == '__main__':
#     create_tables()