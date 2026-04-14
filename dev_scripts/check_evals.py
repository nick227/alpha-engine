import sqlite3
c = sqlite3.connect('data/alpha.db')
r = c.execute('''
    SELECT p.strategy_id, p.horizon, MIN(o.evaluated_at), MAX(o.evaluated_at)
    FROM prediction_outcomes po
    JOIN predictions p ON p.id = po.prediction_id
    WHERE p.horizon IN ('7d', '30d')
    GROUP BY p.strategy_id, p.horizon
''').fetchall()
print('7d/30d evaluated_at range:')
for x in r:
    print(f'{x[0]} {x[1]}: {x[2]} to {x[3]}')
c.close()
