"""Corrección puntual de código en Pacas Hollywood; conserva el ID y sus relaciones.

Ejecutar desde backend: python scripts/rename_na9_to_hl9.py --apply --backup-file /ruta/producto-na9.json
Sin --apply solo muestra la corrección prevista.
"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sqlalchemy import create_engine, text
from app.config import get_company_profiles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--backup-file', type=Path)
    args = parser.parse_args()
    if args.apply and not args.backup_file:
        parser.error('--apply requiere --backup-file')
    profile = next((p for p in get_company_profiles() if p['key'] == 'hollywood_pacas'), None)
    if not profile:
        raise SystemExit('No está configurada la empresa hollywood_pacas')
    engine = create_engine(profile['database_url'], echo=False)
    with engine.begin() as conn:
        rows = list(conn.execute(text("SELECT id, cod_producto, descripcion FROM productos WHERE upper(trim(cod_producto)) IN ('NA9', 'HL9') ORDER BY id FOR UPDATE")).mappings())
        old = [dict(r) for r in rows if r['cod_producto'].strip().upper() == 'NA9']
        new = [dict(r) for r in rows if r['cod_producto'].strip().upper() == 'HL9']
        if not old and len(new) == 1:
            print(json.dumps({'status': 'already_applied', 'product': new[0]}, ensure_ascii=False))
            return
        if len(old) != 1 or new:
            raise SystemExit('Se esperaba exactamente un NA9 y ningún HL9; no se modificó nada')
        before = old[0]
        if args.apply:
            with args.backup_file.open('x', encoding='utf-8') as backup:
                json.dump({'company': profile['key'], 'before': before, 'new_code': 'HL9'}, backup, ensure_ascii=False, indent=2)
            result = conn.execute(text("UPDATE productos SET cod_producto = 'HL9' WHERE id = :id AND cod_producto = :old"), {'id': before['id'], 'old': before['cod_producto']})
            if result.rowcount != 1:
                raise RuntimeError('El producto cambió durante la corrección')
        print(json.dumps({'status': 'applied' if args.apply else 'preview', 'id': before['id'], 'old_code': before['cod_producto'], 'new_code': 'HL9', 'description': before['descripcion']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
