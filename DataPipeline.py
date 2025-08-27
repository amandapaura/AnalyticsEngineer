import traceback


class ETLProcessor:
    
    def __init__(self,source_connector, target_connector):
        self.source_conn = source_connector.connect()
        self.target_conn = target_connector.connect()
        self.source_cursor = self.source_conn.cursor()
        self.target_cursor = self.target_conn.cursor()

    def transfer_data_batch(self, source_query, target_table, target_columns, batch_size=100000):
        print('Starting MySQL extraction')
        self.source_cursor.execute(source_query)

        # Adicionando colunas extras (DATA_INCLUSAO e ULTIMA_DATA_ALTERACAO)
        num_cols = len(target_columns)
        placeholders = ','.join([f':{i+1}' for i in range(num_cols)])
        #print(placeholders)

        print('Starting Oracle loading')
        insert_sql = f"INSERT INTO {target_table} ({', '.join(target_columns)}) VALUES ({placeholders})"
        #print("SQL de inserção:", insert_sql)

        total_inserted = 0
        while True:
            rows = self.source_cursor.fetchmany(batch_size)
            if not rows:
                break

            from datetime import datetime
            #current_time = datetime.now()
            #rows_with_timestamps = [row + (current_time, current_time) for row in rows]

            # DEBUG
            # print(f"SQL: {insert_sql}")
            # print(f"Exemplo de linha: {rows_with_timestamps[0]}")
            # print(f"Número de colunas na linha: {len(rows_with_timestamps[0])}")

            self.target_cursor.executemany(insert_sql, rows)
            self.target_conn.commit()
            total_inserted += len(rows)
            print(f"Inserted {total_inserted} rows...")

        print(f"Transfer completed: {total_inserted} rows inserted. ")


    def execute_proc(self, procedure):
        print(f"Stating {procedure} execution.")
        self.target_cursor.execute(f'BEGIN {procedure}; END;')
        print(f"procedure {procedure} executed.")
        
    def trunc_table(self, table):
        print(f"Stating truncate {table}.")
        self.target_cursor.execute(f'TRUNCATE TABLE {table}')
        print(f"table {table} trucated.")
        
    def close_cursor(self):
        self.source_cursor.close()

        self.target_cursor.close()
