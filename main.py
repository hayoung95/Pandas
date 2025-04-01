from datetime import datetime
import pandas as pd
import os.path
import sqlite3

DB_NAME = "pandas.db"

def get_connection():
    connection = sqlite3.connect(DB_NAME)
    return connection

def main():
    connection = get_connection()
    cursor = connection.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        source_file = 'datafiles/source/bank_transactions.csv'
        source_filename_ext = os.path.basename(source_file)
        source_filename = os.path.splitext(source_filename_ext)[0]
        
        print("#"*5, " Begin Preprocessing   ", "#"*5)
        
        # 전처리부 / 거래일시,은행명,거래유형,거래금액,통화,계좌번호
        datafile = pd.read_csv(source_file, sep=',', encoding='utf-8', header=None, names=['datetime', 'bank', 'type', 'amount', 'currency', 'account'])
        datafile['datetime'] = pd.to_datetime(datafile['datetime']).dt.strftime('%Y%m%d%H%M%S')
        
        lines = datafile.apply(lambda row: ''.join(map(str, row)).replace('-',''), axis=1)
        row_count = len(lines)
        
        # table insert
        runSQL = "INSERT INTO preprocessing (run_timestamp, row_count, source_file) VALUES(?, ?, ?)"
        cursor.execute(runSQL, (now, row_count, source_file))
        
        run_number = cursor.lastrowid
        result_dir = "datafiles/result/"+source_filename+"_"+str(run_number)
        
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        
        with open(result_dir+'/'+source_filename_ext, 'w', encoding="utf-8") as f:
            for i, line in enumerate(lines):
                f.write(line + ('\n' if i < len(lines) - 1 else ''))
                
        # 데이터 프레임 객체인 경우 : .to_csv / 문자열인 경우 : open&write 
        #datafile.to_csv(result_dir + '/bank_transactions.csv', index=False, encoding='utf-8')
            
        # with open(result_dir+'bank_transactions.csv', 'w', encoding="utf-8") as f:
        #     f.write(datafile)
               
        connection.commit()
        print("#"*5, " Success Preprocessing ", "#"*5)
        return True
    
    except Exception as e:
        connection.rollback()
        print(f"전처리 실행 오류 : \n {e}")
        return False
    finally:
        connection.close()

if __name__ == "__main__":
    main()