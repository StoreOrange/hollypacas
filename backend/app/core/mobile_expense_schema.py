"""Upgrade early local expense metadata without changing accounting entries or cash."""
from sqlalchemy import inspect,text


def ensure_mobile_expense_columns(conn):
    if conn.dialect.name=='postgresql':
        conn.execute(text('SELECT pg_advisory_xact_lock(83742027)'))
    if not inspect(conn).has_table('mobile_expenses'):
        return []
    columns={item['name'] for item in inspect(conn).get_columns('mobile_expenses')}
    types={'amount':'NUMERIC(14,2)','currency':'VARCHAR(10)','exchange_rate':'NUMERIC(12,4)'}
    missing=[name for name in types if name not in columns]
    if not missing:return []
    for name in missing:
        conn.execute(text(f'ALTER TABLE mobile_expenses ADD COLUMN {name} {types[name]}'))
    # These fields were added after the original metadata table. Recover only
    # newly-added fields from their linked, unchanged accounting/cash snapshots.
    source="""SELECT e.moneda AS currency,
        COALESCE(NULLIF(e.tasa_cambio,0),NULLIF(e.tasa_sistema,0),NULLIF(r.tasa_cambio,0)) AS exchange_rate,
        CASE WHEN e.moneda='USD' THEN
            CASE WHEN r.monto_usd>0 THEN r.monto_usd
                 WHEN COALESCE(NULLIF(e.tasa_cambio,0),NULLIF(r.tasa_cambio,0))>0
                 THEN ROUND(e.total_debe/COALESCE(NULLIF(e.tasa_cambio,0),NULLIF(r.tasa_cambio,0)),2)
                 ELSE NULL END
            ELSE e.total_debe END AS amount,
        m.id AS expense_id
        FROM mobile_expenses m JOIN accounting_entries e ON e.id=m.entry_id
        LEFT JOIN recibos_caja r ON r.id=m.cash_receipt_id"""
    for name in missing:
        conn.execute(text(f'UPDATE mobile_expenses SET {name}=(SELECT restored.{name} FROM ({source}) restored WHERE restored.expense_id=mobile_expenses.id)'))
    invalid=conn.execute(text("""SELECT COUNT(*) FROM mobile_expenses
        WHERE amount IS NULL OR amount<=0 OR currency IS NULL OR currency NOT IN ('CS','USD')
           OR (currency='USD' AND (exchange_rate IS NULL OR exchange_rate<=0))""")).scalar_one()
    if invalid:raise RuntimeError('Hay gastos sin un soporte contable válido para recuperar su moneda/monto; no se aplicó la actualización.')
    if conn.dialect.name=='postgresql':
        for name in ('amount','currency'):
            conn.execute(text(f'ALTER TABLE mobile_expenses ALTER COLUMN {name} SET NOT NULL'))
    return missing
