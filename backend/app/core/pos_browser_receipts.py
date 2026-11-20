"""Render existing POS receipts into a single browser print document; no sales writes."""
import re
from html.parser import HTMLParser
from html import escape

PRINT_TIMEOUT_SECONDS = 40


class _Receipt(HTMLParser):
    def __init__(self, warehouse=False):
        super().__init__(convert_charrefs=False)
        self.depth = 0
        self.parts = []
        self.done = False
        self.warehouse = warehouse
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if self.done:
            return
        void = tag in {'img', 'br', 'hr', 'input', 'meta', 'link', 'wbr', 'source'}
        if self.skip:
            if not void:
                self.skip += 1
            return
        classes = set(dict(attrs).get('class', '').split())
        if self.depth and self.warehouse and classes.intersection({'pos-money', 'pos-invoice-details', 'pos-commercial-footer'}):
            self.skip = 0 if void else 1
            return
        if not self.depth:
            if tag != 'div' or 'ticket' not in dict(attrs).get('class', '').split():
                return
            self.depth = 1
        elif tag == 'div':
            self.depth += 1
        self.parts.append(self.get_starttag_text())

    def handle_endtag(self, tag):
        if self.skip:
            if tag not in {'img', 'br', 'hr', 'input', 'meta', 'link', 'wbr', 'source'}:
                self.skip -= 1
            return
        if self.depth:
            self.parts.append(f'</{tag}>')
            if tag == 'div':
                self.depth -= 1
                self.done = not self.depth

    def handle_data(self, data):
        if self.depth and not self.skip:
            self.parts.append(data)

    def handle_entityref(self, name):
        if self.depth and not self.skip:
            self.parts.append('&' + name + ';')

    def handle_charref(self, name):
        if self.depth and not self.skip:
            self.parts.append('&#' + name + ';')


def receipt_copies(html, labels, count):
    parser = _Receipt()
    parser.feed(html)
    if not parser.done:
        raise ValueError('No se pudo preparar el comprobante POS')
    original = ''.join(parser.parts)
    receipts = []
    for label in labels or [''] * max(1, min(5, count)):
        receipt = re.sub(r'(<div[^>]*id="pos-copy-label"[^>]*>).*?(</div>)', lambda m: m[1] + escape(label) + m[2], original, count=1, flags=re.S)
        receipt = receipt.replace('id="pos-copy-label"', 'data-pos-copy-label="true"')
        if label == 'EXCLUSIVA DE BODEGA':
            receipt = receipt.replace('class="ticket', 'class="ticket warehouse-dispatch', 1)
            clean = _Receipt(warehouse=True)
            clean.feed(receipt)
            if not clean.done:
                raise ValueError('No se pudo preparar la exclusiva de bodega')
            receipt = ''.join(clean.parts)
        receipts.append(receipt)
    style = re.search(r'<style>(.*?)</style>', html, re.S)
    return receipts, style[1] if style else ''
