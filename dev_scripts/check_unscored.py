import sqlite3
c = sqlite3.connect('data/alpha.db')
r = c.execute("SELECT COUNT(*) FROM predictions p LEFT JOIN prediction_outcomes o ON o.prediction_id = p.id WHERE o.id IS NULL").fetchone()
print(f'Unscored predictions: {r[0]}')

# Check unscored by strategy
print('\nUnscored by strategy:')
r = c.execute('''
    SELECT p.strategy_id, p.horizon, COUNT(*) as cnt
    FROM predictions p
    LEFT JOIN prediction_outcomes o ON o.prediction_id = p.id
    WHERE o.id IS NULL
    GROUP BY p.strategy_id, p.horizon
    ORDER BY cnt DESC
    LIMIT 15
''').fetchall()
for x in r:
    print(f'  {x[0]} {x[1]}: {x[2]}')

c.close()
