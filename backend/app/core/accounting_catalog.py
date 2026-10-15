"""Chart of accounts hierarchy and safe edits, shared by both catalog UIs."""
from collections import defaultdict
from sqlalchemy import func
from ..models.sales import CuentaContable as Account, AccountingEntryLine


def descendants(accounts, item_id):
    children = defaultdict(list)
    for account in accounts:
        children[account.parent_id].append(account.id)
    found, stack = set(), list(children[item_id])
    while stack:
        ident = stack.pop()
        if ident in found:
            continue
        found.add(ident); stack.extend(children[ident])
    return found


def account_paths(accounts):
    by_id = {account.id: account for account in accounts}
    paths = {}
    for account in accounts:
        parts, visited, current = [], set(), account
        while current:
            if current.id in visited:
                parts.append('Ubicación por revisar'); break
            visited.add(current.id)
            parts.append(f'{current.codigo} · {current.nombre}')
            current = by_id.get(current.parent_id)
        paths[account.id] = ' › '.join(reversed(parts))
    return paths


def suggested_code(parent, accounts):
    if not parent:
        return ''
    suffixes = [account.codigo[len(parent.codigo):].lstrip('.') for account in accounts
                if account.parent_id == parent.id and account.codigo.startswith(parent.codigo)]
    seq = max([int(s) for s in suffixes if s.isdigit()] or [0]) + 1
    width = 1 if parent.codigo.isdigit() and len(parent.codigo) == 1 else 2
    separator = '' if parent.codigo.isdigit() else '.'
    taken = {account.codigo.lower() for account in accounts}
    code = parent.codigo + separator + str(seq).zfill(width)
    while code.lower() in taken:
        seq += 1; code = parent.codigo + separator + str(seq).zfill(width)
    return code if len(code) <= 20 else ''


def save_account(db, *, account_id=None, codigo, nombre, descripcion='', tipo, naturaleza, parent_id=None, active=True):
    codigo, nombre, descripcion = codigo.strip(), nombre.strip(), descripcion.strip()
    tipo, naturaleza = tipo.strip().upper(), naturaleza.strip().upper()
    if not codigo or len(codigo) > 20 or not nombre or len(nombre) > 160 or len(descripcion) > 2000:
        raise ValueError('Código y nombre son obligatorios (máximo 20 y 160 caracteres); descripción de hasta 2000 caracteres.')
    if tipo not in {'BALANCE','RESULTADO'} or naturaleza not in {'DEBE','HABER'}:
        raise ValueError('Selecciona tipo y naturaleza válidos.')
    accounts = db.query(Account).order_by(Account.id).all()
    by_id = {account.id: account for account in accounts}
    item = by_id.get(account_id) if account_id else None
    if account_id and not item:
        raise ValueError('Cuenta no encontrada.')
    if any(account.codigo.lower()==codigo.lower() and account.id != account_id for account in accounts):
        raise ValueError('Este código ya existe en el catálogo.')
    parent = by_id.get(parent_id) if parent_id else None
    if parent_id and not parent:
        raise ValueError('Selecciona una cuenta padre válida.')
    if parent and active and not parent.activo:
        raise ValueError('No puedes activar una subcuenta bajo una cuenta padre inactiva.')
    if parent and parent.tipo != tipo:
        raise ValueError('La subcuenta debe conservar el tipo contable de su cuenta padre.')
    if parent and (not codigo.startswith(parent.codigo) or codigo == parent.codigo):
        raise ValueError(f'El código de la subcuenta debe empezar por {parent.codigo} y añadir su propio segmento.')
    affected = descendants(accounts, item.id) if item else set()
    if parent and (parent.id == account_id or parent.id in affected):
        raise ValueError('Una cuenta no puede ubicarse dentro de sí misma o de sus subcuentas.')
    if item and not active and any(by_id[ident].activo for ident in affected):
        raise ValueError('Desactiva primero las subcuentas activas antes de desactivar su cuenta padre.')
    if item and affected and codigo != item.codigo:
        raise ValueError('Una cuenta con subcuentas conserva su código para mantener su ubicación. Puedes editar su nombre y descripción.')
    if item and (codigo != item.codigo or tipo != item.tipo):
        if db.query(AccountingEntryLine.id).filter(AccountingEntryLine.cuenta_id.in_({item.id}|affected)).first():
            raise ValueError('Una cuenta con movimientos conserva código y tipo contable. Puedes editar su nombre y descripción, o crear una subcuenta.')
    if item and any(by_id[ident].tipo != tipo for ident in affected):
        raise ValueError('El tipo contable debe ser compatible con las subcuentas existentes.')
    parents = {account.id: account.parent_id for account in accounts}
    item_key = item.id if item else -1
    parents[item_key] = parent_id
    def depth(ident):
        seen, level, current = set(), 0, ident
        while current is not None:
            if current in seen or current not in parents:
                raise ValueError('La ubicación contable contiene una referencia circular o inválida.')
            seen.add(current); level += 1; current = parents[current]
        return level
    levels = {ident: depth(ident) for ident in affected|{item_key}}
    if max(levels.values()) > 4:
        raise ValueError('La ubicación admite hasta 4 niveles; considera también las subcuentas que ya existen.')
    if item is None:
        item = Account()
        db.add(item)
    item.codigo, item.nombre, item.descripcion = codigo, nombre, descripcion or None
    item.tipo, item.naturaleza, item.parent_id, item.activo = tipo, naturaleza, parent_id, active
    item.nivel = levels[item_key]
    for ident in affected:
        by_id[ident].nivel = levels[ident]
    db.flush()
    return item


def chart_rows(accounts):
    children = defaultdict(list)
    by_id = {account.id:account for account in accounts}
    for account in accounts:
        children[account.parent_id].append(account)
    paths, rows, visited = account_paths(accounts), [], set()
    def walk(account, depth):
        if account.id in visited:
            return
        visited.add(account.id); rows.append(dict(account=account, depth=depth, path=paths[account.id]))
        for child in sorted(children[account.id],key=lambda a:a.codigo):
            walk(child,depth+1)
    for account in sorted(accounts,key=lambda a:a.codigo):
        if not account.parent_id or account.parent_id not in by_id:
            walk(account,0)
    for account in sorted(accounts,key=lambda a:a.codigo):
        if account.id not in visited:
            walk(account,0)
    return rows
