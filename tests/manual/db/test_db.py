import sqlite3

conn = sqlite3.connect('data/alpha.db')
cursor = conn.cursor()
cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
tables = cursor.fetchall()
print('Tables:', tables)

# Check if predictions table exists
cursor.execute('SELECT COUNT(*) FROM predictions')
count = cursor.fetchone()
print('Predictions count:', count)

conn.close()
