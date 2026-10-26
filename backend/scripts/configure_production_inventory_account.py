"""Configure the separate monetary purchase account in Holl Pacas only.

Preview by default. --apply requires an exclusive JSON audit file.
Does not create entries, warehouse records, products or stock movements.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sqlalchemy import create_engine, text
from app.config import get_active_company_key, get_active_database_url

CODE = '1112'
NAME = 'Inventario de producción en proceso'
DESCRIPTION = ('Control monetario de inversión en compras para producción. Separado de 1104 Inventarios. '
               'No representa existencias físicas ni modifica costos de bodegas. Aplicación a producción pendiente de fase posterior.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--audit-file', type=Path)
    args = parser.parse_args()
    if args.apply and not args.audit_file:
        parser.error('--apply requiere --audit-file')
    if get_active_company_key() != 'hollywood_pacas':
        raise SystemExit('Solo se permite configurar hollywood_pacas')
    with create_engine(get_active_database_url()).begin() as conn:
        conn.execute(text('SELECT pg_advisory_xact_lock(83742026)'))
        existing = conn.execute(text('SELECT * FROM cuentas_contables WHERE codigo=:code'), {'code': CODE}).mappings().first()
        parent = conn.execute(text("SELECT * FROM cuentas_contables WHERE codigo='11'")).mappings().one()
        if parent['tipo'] != 'BALANCE' or not parent['activo']:
            raise SystemExit('Revisar Activo Corriente antes de continuar')
        if existing:
            if not (existing['nombre'] == NAME and existing['tipo'] == 'BALANCE' and existing['naturaleza'] == 'DEBE'
                    and existing['activo'] and existing['parent_id'] == parent['id']):
                raise SystemExit('El código 1112 ya está ocupado o difiere de la configuración prevista')
            print(json.dumps({'status': 'already_configured', 'id': existing['id'], 'code': CODE}, ensure_ascii=False))
            return
        duplicate = conn.execute(text('SELECT id FROM cuentas_contables WHERE lower(nombre)=lower(:name)'), {'name': NAME}).first()
        if duplicate:
            raise SystemExit('Ya existe una cuenta con este nombre; revisar sin duplicar')
        planned = {'codigo': CODE, 'nombre': NAME, 'descripcion': DESCRIPTION, 'tipo': 'BALANCE',
                   'naturaleza': 'DEBE', 'parent_id': parent['id'], 'nivel': parent['nivel'] + 1, 'activo': True}
        if args.apply:
            with args.audit_file.open('x', encoding='utf-8') as audit:
                json.dump({'company': 'hollywood_pacas', 'at': datetime.now(timezone.utc).isoformat(),
                           'before': None, 'planned': planned, 'operation': 'create_account_only'}, audit, ensure_ascii=False, indent=2)
            account_id = conn.execute(text('INSERT INTO cuentas_contables (codigo,nombre,descripcion,tipo,naturaleza,parent_id,nivel,activo) '
                                          'VALUES (:codigo,:nombre,:descripcion,:tipo,:naturaleza,:parent_id,:nivel,:activo) RETURNING id'), planned).scalar_one()
        else:
            account_id = None
        print(json.dumps({'status': 'applied' if args.apply else 'preview', 'id': account_id, 'account': planned}, ensure_ascii=False))


if __name__ == '__main__':
    main()
