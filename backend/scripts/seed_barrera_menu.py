from __future__ import annotations

import os
import re
import unicodedata
from decimal import Decimal, ROUND_HALF_UP
from html import escape
from pathlib import Path

from sqlalchemy import create_engine, text


ROOT_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = ROOT_DIR / "app" / "static"
ASSET_DIR = STATIC_DIR / "product_assets" / "barrera" / "menu"
ENV_PATH = ROOT_DIR / ".env"


LINEAS = [
    ("LB_CORTES", "Cortes Premium"),
    ("LB_NICA", "Tan Nica Como El Rio San Juan"),
    ("LB_COMPARTIR", "Compartiendo Sabe Mejor"),
    ("LB_TOSTONES", "Tostones y Acompanantes"),
    ("LB_EXTRAS", "Extras"),
    ("LB_BEBIDAS", "Bebidas"),
]


MENU = [
    ("Cortes Premium", "Churrasco", Decimal("450.00"), "steak"),
    ("Cortes Premium", "Puyazo", Decimal("450.00"), "steak"),
    ("Cortes Premium", "New York", Decimal("590.00"), "steak"),
    ("Cortes Premium", "T Bone", Decimal("690.00"), "steak"),
    ("Cortes Premium", "Rib Eye", Decimal("590.00"), "steak"),
    ("Cortes Premium", "Poterhouse", Decimal("650.00"), "steak"),
    ("Tan Nica Como El Rio San Juan", "Carne Asada", Decimal("260.00"), "grill"),
    ("Tan Nica Como El Rio San Juan", "Costilla de Res Premium Asadas", Decimal("300.00"), "grill"),
    ("Tan Nica Como El Rio San Juan", "Cerdo Asado", Decimal("260.00"), "grill"),
    ("Tan Nica Como El Rio San Juan", "Pollo Asado", Decimal("260.00"), "grill"),
    ("Tan Nica Como El Rio San Juan", "Brocheta", Decimal("220.00"), "skewer"),
    ("Tan Nica Como El Rio San Juan", "Brocheta Mixta", Decimal("280.00"), "skewer"),
    ("Compartiendo Sabe Mejor", "Parrillada Mixta", Decimal("600.00"), "platter"),
    ("Compartiendo Sabe Mejor", "Surtido La Barrera", Decimal("1100.00"), "platter"),
    ("Compartiendo Sabe Mejor", "Mix de Chorizos Parrilleros", Decimal("250.00"), "platter"),
    ("Compartiendo Sabe Mejor", "Trozos de Carne Res, Pollo o Cerdo", Decimal("250.00"), "platter"),
    ("Tostones y Acompanantes", "Tostones con Carne", Decimal("180.00"), "snack"),
    ("Tostones y Acompanantes", "Tostones con Carne Asada", Decimal("250.00"), "snack"),
    ("Tostones y Acompanantes", "Tostones con Queso", Decimal("140.00"), "snack"),
    ("Tostones y Acompanantes", "Tostones con Chorizo Parrillero", Decimal("160.00"), "snack"),
    ("Tostones y Acompanantes", "Tostones Mixtos", Decimal("250.00"), "snack"),
    ("Tostones y Acompanantes", "Tajadas con Queso", Decimal("100.00"), "snack"),
    ("Tostones y Acompanantes", "Maduro con Queso", Decimal("100.00"), "snack"),
    ("Tostones y Acompanantes", "Salchipapa", Decimal("160.00"), "snack"),
    ("Extras", "Gallo Pinto", Decimal("50.00"), "extra"),
    ("Extras", "Queso", Decimal("50.00"), "extra"),
    ("Extras", "Tajadas, Tostones o Maduro", Decimal("50.00"), "extra"),
    ("Extras", "Papas Fritas", Decimal("60.00"), "extra"),
    ("Bebidas", "Te de Jamaica", Decimal("50.00"), "drink"),
    ("Bebidas", "Jugo de Naranja", Decimal("50.00"), "drink"),
    ("Bebidas", "Te Frio de Limon", Decimal("50.00"), "drink"),
    ("Bebidas", "Gaseosa en Lata", Decimal("40.00"), "drink"),
    ("Bebidas", "Gaseosa Botella 500ML", Decimal("50.00"), "drink"),
    ("Bebidas", "Agua Fuente Pura 600ML", Decimal("40.00"), "drink"),
    ("Bebidas", "Agua Fuente Pura 1 Litro", Decimal("60.00"), "drink"),
]


PALETTES = {
    "steak": ("#2a120e", "#6f2618", "#f7b267", "#ff6b35"),
    "grill": ("#2e160f", "#7a3518", "#ffd166", "#ff7b54"),
    "skewer": ("#22170d", "#895129", "#f4d35e", "#ff8c42"),
    "platter": ("#23161e", "#5f264a", "#f4b6c2", "#ff6f91"),
    "snack": ("#2c220f", "#8d6a1f", "#ffe28a", "#ffb703"),
    "extra": ("#1d2232", "#40577a", "#b9d6f2", "#4ea8de"),
    "drink": ("#11263c", "#1f5c8d", "#8fd3ff", "#4cc9f0"),
}


def load_env() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    clean = re.sub(r"[^a-z0-9]+", "-", ascii_only.lower()).strip("-")
    return clean or "item"


def product_code(name: str) -> str:
    slug = slugify(name).upper().replace("-", "_")
    return f"LB_{slug[:52]}"


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def pick_palette(style: str) -> tuple[str, str, str, str]:
    return PALETTES.get(style, PALETTES["extra"])


def detect_variant(name: str, style: str) -> str:
    label = slugify(name)
    if style == "drink":
        if "agua" in label:
            return "water"
        if "jugo" in label or "naranja" in label:
            return "juice"
        if "jamaica" in label:
            return "tea"
        if "lata" in label:
            return "can"
        if "botella" in label or "500ml" in label or "litro" in label:
            return "bottle"
        return "glass"
    if style == "steak":
        if "t-bone" in label or "porterhouse" in label or "poterhouse" in label:
            return "bone_steak"
        if "rib-eye" in label or "new-york" in label:
            return "marbled_steak"
        if "puyazo" in label:
            return "long_steak"
        return "classic_steak"
    if style == "grill":
        if "costilla" in label:
            return "ribs"
        if "pollo" in label:
            return "chicken"
        if "cerdo" in label:
            return "pork"
        return "grill_plate"
    if style == "skewer":
        if "mixta" in label:
            return "mixed_skewer"
        return "single_skewer"
    if style == "platter":
        if "surtido" in label:
            return "grand_platter"
        if "chorizo" in label:
            return "sausages"
        if "trozos" in label:
            return "meat_bites"
        return "mixed_platter"
    if style == "snack":
        if "queso" in label:
            return "cheese_snack"
        if "salchipapa" in label:
            return "fries_snack"
        if "maduro" in label:
            return "sweet_plantain"
        if "tajadas" in label:
            return "sliced_plantain"
        if "chorizo" in label:
            return "sausage_snack"
        if "mixtos" in label:
            return "mixed_snack"
        return "meat_snack"
    if style == "extra":
        if "gallo" in label:
            return "rice_beans"
        if "queso" in label:
            return "cheese_side"
        if "papas" in label:
            return "fries_side"
        return "plantain_side"
    return "default"


def render_center_art(style: str, variant: str) -> str:
    if style == "drink" and variant == "water":
        return """
  <g transform="translate(0 2)">
    <rect x="280" y="116" width="82" height="178" rx="30" fill="rgba(255,255,255,.14)" stroke="rgba(215,245,255,.70)" stroke-width="6"/>
    <path d="M294 146h54v124a16 16 0 0 1-16 16h-22a16 16 0 0 1-16-16z" fill="rgba(130,214,255,.34)"/>
    <path d="M320 98v34" stroke="#d7f6ff" stroke-width="8" stroke-linecap="round"/>
    <circle cx="320" cy="206" r="9" fill="#eefcff">
      <animate attributeName="cy" values="236;162;236" dur="2.2s" repeatCount="indefinite"/>
      <animate attributeName="opacity" values=".2;1;.2" dur="2.2s" repeatCount="indefinite"/>
    </circle>
    <circle cx="340" cy="228" r="7" fill="#eefcff">
      <animate attributeName="cy" values="244;176;244" dur="1.9s" repeatCount="indefinite"/>
      <animate attributeName="opacity" values=".15;1;.15" dur="1.9s" repeatCount="indefinite"/>
    </circle>
  </g>
"""
    if style == "drink" and variant == "juice":
        return """
  <g transform="translate(0 4)">
    <rect x="262" y="146" width="116" height="136" rx="24" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.64)" stroke-width="6"/>
    <path d="M282 164h76l-10 92a16 16 0 0 1-16 14h-24a16 16 0 0 1-16-14z" fill="rgba(255,178,74,.50)"/>
    <circle cx="320" cy="132" r="26" fill="#ffba49"/>
    <circle cx="320" cy="132" r="16" fill="#ffdb88"/>
    <path d="M336 112c18-10 32-8 42 4-16 4-26 14-36 30" fill="none" stroke="#6ac46a" stroke-width="8" stroke-linecap="round"/>
  </g>
"""
    if style == "drink" and variant in {"tea", "glass"}:
        return """
  <g transform="translate(0 4)">
    <rect x="266" y="138" width="108" height="138" rx="26" fill="rgba(255,255,255,.18)" stroke="rgba(255,255,255,.66)" stroke-width="6"/>
    <path d="M286 154h68l-10 102a12 12 0 0 1-12 10h-24a12 12 0 0 1-12-10z" fill="rgba(255,255,255,.28)"/>
    <path d="M320 98c22 16 22 34 0 56" fill="none" stroke="#d8f3ff" stroke-width="8" stroke-linecap="round">
      <animate attributeName="d" dur="2.8s" repeatCount="indefinite"
        values="M320 98c22 16 22 34 0 56;M320 98c10 20 10 36 0 56;M320 98c22 16 22 34 0 56"/>
    </path>
  </g>
"""
    if style == "drink" and variant in {"can", "bottle"}:
        return """
  <g transform="translate(0 2)">
    <rect x="286" y="116" width="68" height="166" rx="24" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.68)" stroke-width="6"/>
    <rect x="298" y="132" width="44" height="116" rx="18" fill="rgba(255,255,255,.20)"/>
    <rect x="304" y="108" width="32" height="14" rx="6" fill="#dcefff"/>
    <path d="M298 162h44" stroke="rgba(255,255,255,.64)" stroke-width="6" stroke-linecap="round"/>
    <path d="M298 194h44" stroke="rgba(255,255,255,.52)" stroke-width="6" stroke-linecap="round"/>
    <path d="M298 226h44" stroke="rgba(255,255,255,.46)" stroke-width="6" stroke-linecap="round"/>
    <circle cx="320" cy="210" r="58" fill="rgba(76,201,240,.18)">
      <animate attributeName="r" values="52;62;52" dur="2.6s" repeatCount="indefinite"/>
    </circle>
  </g>
"""
    if style == "steak" and variant == "bone_steak":
        return """
  <g transform="translate(0 0)">
    <ellipse cx="320" cy="224" rx="116" ry="78" fill="rgba(255,255,255,.18)" stroke="rgba(255,255,255,.65)" stroke-width="6"/>
    <path d="M246 230c0-48 38-82 92-82 22 0 36 8 48 18 18 16 28 38 28 60 0 44-30 82-86 82-52 0-82-24-82-78z" fill="rgba(255,255,255,.24)"/>
    <path d="M342 178c10-10 24-12 36-4 12 8 14 24 4 36l-20 22c-10 10-24 12-36 4-12-8-14-24-4-36z" fill="#fff4df"/>
    <circle cx="350" cy="214" r="12" fill="#f1debe"/>
  </g>
"""
    if style == "steak" and variant == "marbled_steak":
        return """
  <g transform="translate(0 2)">
    <ellipse cx="320" cy="222" rx="108" ry="74" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.65)" stroke-width="6"/>
    <path d="M252 236c0-46 38-78 88-78 48 0 82 28 82 72 0 46-34 80-84 80-52 0-86-28-86-74z" fill="rgba(255,255,255,.24)"/>
    <path d="M282 214c20-12 38-12 58 0" stroke="#fff2df" stroke-width="8" stroke-linecap="round"/>
    <path d="M300 248c22-8 40-8 60 4" stroke="#ffe7d6" stroke-width="7" stroke-linecap="round"/>
    <path d="M274 186c18-10 38-12 66-2" stroke="#ffe7d6" stroke-width="6" stroke-linecap="round"/>
  </g>
"""
    if style == "steak" and variant == "long_steak":
        return """
  <g transform="translate(0 8)">
    <ellipse cx="320" cy="230" rx="124" ry="62" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.64)" stroke-width="6"/>
    <rect x="228" y="176" width="184" height="102" rx="48" fill="rgba(255,255,255,.24)" transform="rotate(-8 320 226)"/>
    <ellipse cx="334" cy="218" rx="24" ry="16" fill="rgba(255,255,255,.56)"/>
  </g>
"""
    if style in {"steak", "grill"} and variant in {"classic_steak", "grill_plate"}:
        return """
  <g transform="translate(0 2)">
    <ellipse cx="320" cy="222" rx="104" ry="72" fill="rgba(255,255,255,.18)" stroke="rgba(255,255,255,.65)" stroke-width="6"/>
    <path d="M256 238c0-44 34-78 74-78 28 0 42 12 54 28 10 12 14 26 14 42 0 44-34 78-74 78-42 0-68-26-68-70z" fill="rgba(255,255,255,.25)"/>
    <ellipse cx="338" cy="214" rx="26" ry="18" fill="rgba(255,255,255,.58)"/>
    <path d="M246 142c-10-18 2-36 18-42-2 18 10 26 18 40 8 10 6 26-6 34-14-4-22-16-30-32z" fill="#ffd37a">
      <animateTransform attributeName="transform" type="translate" dur="1.5s" repeatCount="indefinite" values="0 0;0 -6;0 0"/>
    </path>
    <path d="M288 126c-8-20 6-38 20-46 0 18 12 30 18 44 6 12 0 24-12 30-12-6-20-14-26-28z" fill="#ff8f5a">
      <animateTransform attributeName="transform" type="translate" dur="1.2s" repeatCount="indefinite" values="0 0;0 -8;0 0"/>
    </path>
    <path d="M332 126c-8-20 6-38 20-46 0 18 12 30 18 44 6 12 0 24-12 30-12-6-20-14-26-28z" fill="#ffe29a">
      <animateTransform attributeName="transform" type="translate" dur="1.7s" repeatCount="indefinite" values="0 0;0 -7;0 0"/>
    </path>
  </g>
"""
    if style == "grill" and variant == "ribs":
        return """
  <g transform="translate(0 4)">
    <ellipse cx="320" cy="236" rx="116" ry="58" fill="rgba(255,255,255,.15)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <rect x="236" y="176" width="168" height="86" rx="36" fill="rgba(255,255,255,.22)"/>
    <path d="M260 168v102M292 164v108M324 164v108M356 168v102" stroke="#fff2da" stroke-width="12" stroke-linecap="round"/>
  </g>
"""
    if style == "grill" and variant == "chicken":
        return """
  <g transform="translate(0 0)">
    <ellipse cx="320" cy="228" rx="110" ry="68" fill="rgba(255,255,255,.15)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <path d="M262 238c0-40 24-70 60-82 34-10 82 22 82 68 0 44-34 76-82 76-34 0-60-22-60-62z" fill="rgba(255,255,255,.24)"/>
    <circle cx="378" cy="194" r="12" fill="#fff2de"/>
    <circle cx="396" cy="178" r="9" fill="#fff2de"/>
  </g>
"""
    if style == "grill" and variant == "pork":
        return """
  <g transform="translate(0 4)">
    <ellipse cx="320" cy="228" rx="114" ry="72" fill="rgba(255,255,255,.15)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <path d="M248 238c0-44 34-78 84-78 50 0 86 30 86 74 0 44-34 78-86 78-50 0-84-26-84-74z" fill="rgba(255,255,255,.22)"/>
    <circle cx="344" cy="218" r="22" fill="rgba(255,255,255,.54)"/>
  </g>
"""
    if style == "skewer" and variant == "mixed_skewer":
        return """
  <g transform="translate(0 0)">
    <rect x="192" y="196" width="256" height="34" rx="17" fill="rgba(255,255,255,.24)"/>
    <path d="M208 212h224" stroke="#f4d8a8" stroke-width="8" stroke-linecap="round"/>
    <circle cx="238" cy="212" r="22" fill="rgba(255,214,118,.92)"/>
    <circle cx="286" cy="212" r="22" fill="rgba(255,126,95,.92)"/>
    <circle cx="334" cy="212" r="22" fill="rgba(255,255,255,.64)"/>
    <circle cx="382" cy="212" r="22" fill="rgba(255,182,79,.94)"/>
  </g>
"""
    if style in {"skewer", "platter"} and variant == "single_skewer":
        return """
  <g transform="translate(0 0)">
    <rect x="202" y="196" width="236" height="30" rx="15" fill="rgba(255,255,255,.24)"/>
    <path d="M216 210h208" stroke="#f4d8a8" stroke-width="8" stroke-linecap="round"/>
    <circle cx="242" cy="210" r="24" fill="rgba(255,255,255,.68)"/>
    <circle cx="294" cy="210" r="22" fill="rgba(255,196,112,.92)"/>
    <circle cx="346" cy="210" r="22" fill="rgba(255,255,255,.62)"/>
    <circle cx="398" cy="210" r="24" fill="rgba(255,126,95,.92)"/>
  </g>
"""
    if style == "platter" and variant == "grand_platter":
        return """
  <g transform="translate(0 4)">
    <ellipse cx="320" cy="232" rx="136" ry="74" fill="rgba(255,255,255,.15)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <circle cx="260" cy="214" r="26" fill="rgba(255,255,255,.66)"/>
    <circle cx="316" cy="202" r="26" fill="rgba(255,179,148,.92)"/>
    <circle cx="372" cy="216" r="26" fill="rgba(255,214,118,.92)"/>
    <circle cx="294" cy="254" r="24" fill="rgba(255,255,255,.54)"/>
    <circle cx="348" cy="252" r="24" fill="rgba(255,111,145,.88)"/>
  </g>
"""
    if style == "platter" and variant == "sausages":
        return """
  <g transform="translate(0 8)">
    <ellipse cx="320" cy="234" rx="124" ry="66" fill="rgba(255,255,255,.15)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <rect x="236" y="190" width="58" height="24" rx="12" fill="rgba(255,140,97,.94)" transform="rotate(-12 265 202)"/>
    <rect x="292" y="204" width="58" height="24" rx="12" fill="rgba(255,170,116,.94)" transform="rotate(6 321 216)"/>
    <rect x="350" y="190" width="58" height="24" rx="12" fill="rgba(255,111,145,.92)" transform="rotate(18 379 202)"/>
  </g>
"""
    if style == "platter" and variant == "meat_bites":
        return """
  <g transform="translate(0 6)">
    <ellipse cx="320" cy="232" rx="126" ry="68" fill="rgba(255,255,255,.15)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <rect x="248" y="184" width="42" height="42" rx="12" fill="rgba(255,196,112,.92)" transform="rotate(-10 269 205)"/>
    <rect x="300" y="198" width="42" height="42" rx="12" fill="rgba(255,111,145,.92)" transform="rotate(8 321 219)"/>
    <rect x="352" y="184" width="42" height="42" rx="12" fill="rgba(255,255,255,.58)" transform="rotate(14 373 205)"/>
    <rect x="278" y="238" width="42" height="42" rx="12" fill="rgba(255,170,116,.92)" transform="rotate(-8 299 259)"/>
    <rect x="332" y="238" width="42" height="42" rx="12" fill="rgba(255,214,118,.90)" transform="rotate(10 353 259)"/>
  </g>
"""
    if style in {"skewer", "platter"}:
        return """
  <g transform="translate(0 0)">
    <rect x="202" y="196" width="236" height="30" rx="15" fill="rgba(255,255,255,.24)"/>
    <path d="M216 210h208" stroke="#f4d8a8" stroke-width="8" stroke-linecap="round"/>
    <circle cx="242" cy="210" r="24" fill="rgba(255,255,255,.68)"/>
    <circle cx="294" cy="210" r="22" fill="rgba(255,196,112,.92)"/>
    <circle cx="346" cy="210" r="22" fill="rgba(255,255,255,.62)"/>
    <circle cx="398" cy="210" r="24" fill="rgba(255,126,95,.92)"/>
    <path d="M252 144c-10-20 6-38 20-46 0 18 12 30 18 44 6 12 0 24-12 30-12-6-20-14-26-28z" fill="#ffe08a">
      <animateTransform attributeName="transform" type="translate" dur="1.35s" repeatCount="indefinite" values="0 0;0 -8;0 0"/>
    </path>
    <path d="M324 130c-10-18 2-36 18-42-2 18 10 26 18 40 8 10 6 26-6 34-14-4-22-16-30-32z" fill="#ff9b54">
      <animateTransform attributeName="transform" type="translate" dur="1.55s" repeatCount="indefinite" values="0 0;0 -6;0 0"/>
    </path>
  </g>
"""
    if style == "snack" and variant == "cheese_snack":
        return """
  <g transform="translate(0 10)">
    <ellipse cx="320" cy="238" rx="112" ry="56" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.60)" stroke-width="6"/>
    <rect x="244" y="176" width="40" height="88" rx="16" fill="rgba(255,227,130,.96)" transform="rotate(-18 264 220)"/>
    <rect x="292" y="164" width="40" height="92" rx="16" fill="rgba(255,235,156,.96)" transform="rotate(-4 312 210)"/>
    <path d="M344 176l46 32-46 32z" fill="#fff1a6"/>
    <circle cx="356" cy="206" r="4" fill="#ffd54f"/>
    <circle cx="370" cy="222" r="4" fill="#ffd54f"/>
  </g>
"""
    if style == "snack" and variant == "fries_snack":
        return """
  <g transform="translate(0 8)">
    <ellipse cx="320" cy="238" rx="108" ry="54" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.60)" stroke-width="6"/>
    <path d="M272 184h96l-8 76h-80z" fill="rgba(255,111,61,.88)"/>
    <rect x="282" y="150" width="12" height="72" rx="6" fill="#ffd166" transform="rotate(-8 288 186)"/>
    <rect x="300" y="144" width="12" height="82" rx="6" fill="#ffe08a" transform="rotate(-2 306 185)"/>
    <rect x="320" y="148" width="12" height="78" rx="6" fill="#ffd166" transform="rotate(4 326 187)"/>
    <rect x="338" y="152" width="12" height="72" rx="6" fill="#ffe08a" transform="rotate(10 344 188)"/>
  </g>
"""
    if style == "snack" and variant in {"sweet_plantain", "sliced_plantain"}:
        return """
  <g transform="translate(0 10)">
    <ellipse cx="320" cy="238" rx="114" ry="56" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.60)" stroke-width="6"/>
    <ellipse cx="280" cy="220" rx="26" ry="54" fill="rgba(255,191,94,.96)" transform="rotate(-28 280 220)"/>
    <ellipse cx="330" cy="220" rx="26" ry="54" fill="rgba(255,210,126,.96)" transform="rotate(6 330 220)"/>
    <ellipse cx="378" cy="222" rx="24" ry="50" fill="rgba(255,172,84,.96)" transform="rotate(24 378 222)"/>
  </g>
"""
    if style == "snack" and variant == "sausage_snack":
        return """
  <g transform="translate(0 10)">
    <ellipse cx="320" cy="238" rx="112" ry="56" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.60)" stroke-width="6"/>
    <rect x="244" y="176" width="36" height="86" rx="16" fill="rgba(255,227,130,.96)" transform="rotate(-18 262 219)"/>
    <rect x="290" y="162" width="36" height="92" rx="16" fill="rgba(255,199,94,.96)" transform="rotate(-4 308 208)"/>
    <rect x="340" y="190" width="62" height="22" rx="11" fill="rgba(255,126,95,.94)" transform="rotate(12 371 201)"/>
    <rect x="336" y="224" width="62" height="22" rx="11" fill="rgba(255,111,145,.92)" transform="rotate(-8 367 235)"/>
  </g>
"""
    if style == "snack" and variant == "mixed_snack":
        return """
  <g transform="translate(0 8)">
    <ellipse cx="320" cy="238" rx="116" ry="58" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.60)" stroke-width="6"/>
    <rect x="238" y="172" width="34" height="84" rx="16" fill="rgba(255,227,130,.96)" transform="rotate(-18 255 210)"/>
    <rect x="282" y="158" width="34" height="96" rx="16" fill="rgba(255,199,94,.96)" transform="rotate(-6 299 202)"/>
    <path d="M346 184l42 28-42 28z" fill="#fff1a6"/>
    <rect x="334" y="238" width="58" height="22" rx="11" fill="rgba(255,111,145,.92)" transform="rotate(8 363 249)"/>
  </g>
"""
    if style == "snack":
        return """
  <g transform="translate(0 6)">
    <ellipse cx="320" cy="236" rx="112" ry="56" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.60)" stroke-width="6"/>
    <rect x="238" y="168" width="34" height="84" rx="16" fill="rgba(255,227,130,.96)" transform="rotate(-18 255 210)"/>
    <rect x="282" y="154" width="34" height="96" rx="16" fill="rgba(255,199,94,.96)" transform="rotate(-6 299 202)"/>
    <rect x="326" y="158" width="34" height="94" rx="16" fill="rgba(255,235,156,.96)" transform="rotate(8 343 205)"/>
    <rect x="370" y="174" width="34" height="80" rx="16" fill="rgba(255,184,28,.96)" transform="rotate(18 387 214)"/>
    <circle cx="320" cy="126" r="14" fill="#fff4c2">
      <animate attributeName="r" values="12;16;12" dur="1.4s" repeatCount="indefinite"/>
    </circle>
  </g>
"""
    if style == "extra" and variant == "rice_beans":
        return """
  <g transform="translate(0 8)">
    <circle cx="320" cy="214" r="88" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <circle cx="290" cy="206" r="18" fill="rgba(255,255,255,.68)"/>
    <circle cx="316" cy="196" r="18" fill="rgba(255,255,255,.78)"/>
    <circle cx="344" cy="214" r="18" fill="rgba(104,76,60,.88)"/>
    <circle cx="308" cy="234" r="18" fill="rgba(104,76,60,.82)"/>
    <circle cx="338" cy="240" r="18" fill="rgba(255,255,255,.70)"/>
  </g>
"""
    if style == "extra" and variant == "cheese_side":
        return """
  <g transform="translate(0 8)">
    <circle cx="320" cy="214" r="84" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <path d="M274 178l98 28-82 56z" fill="#fff1a6"/>
    <circle cx="318" cy="212" r="6" fill="#ffd54f"/>
    <circle cx="336" cy="224" r="5" fill="#ffd54f"/>
    <circle cx="300" cy="230" r="5" fill="#ffd54f"/>
  </g>
"""
    if style == "extra" and variant == "fries_side":
        return """
  <g transform="translate(0 10)">
    <circle cx="320" cy="214" r="84" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <path d="M286 184h68l-8 64h-52z" fill="rgba(78,168,222,.88)"/>
    <rect x="292" y="158" width="10" height="62" rx="5" fill="#ffd166"/>
    <rect x="308" y="152" width="10" height="70" rx="5" fill="#ffe08a"/>
    <rect x="324" y="156" width="10" height="66" rx="5" fill="#ffd166"/>
    <rect x="340" y="160" width="10" height="60" rx="5" fill="#ffe08a"/>
  </g>
"""
    return """
  <g transform="translate(0 8)">
    <circle cx="320" cy="214" r="84" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.62)" stroke-width="6"/>
    <circle cx="288" cy="208" r="22" fill="rgba(255,255,255,.62)"/>
    <circle cx="342" cy="194" r="18" fill="rgba(255,255,255,.82)"/>
    <circle cx="336" cy="240" r="20" fill="rgba(78,168,222,.88)"/>
    <path d="M320 112c16 12 24 28 24 44s-8 30-24 42c-16-12-24-26-24-42s8-32 24-44z" fill="#bfe9ff">
      <animateTransform attributeName="transform" type="translate" dur="1.8s" repeatCount="indefinite" values="0 0;0 -8;0 0"/>
    </path>
  </g>
"""


def build_svg(name: str, category: str, price_cs: Decimal, style: str) -> str:
    bg_start, bg_end, accent, glow = pick_palette(style)
    slug = slugify(name)
    variant = detect_variant(name, style)
    short_code = slug.upper().replace("-", " ")[:18]
    safe_name = escape(name)
    safe_category = escape(category.upper())
    safe_price = escape(f"C$ {price_cs:.2f}")
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 480" role="img" aria-label="{safe_name}">
  <defs>
    <linearGradient id="bg-{slug}" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="{bg_start}">
        <animate attributeName="stop-color" values="{bg_start};{bg_end};{bg_start}" dur="7s" repeatCount="indefinite"/>
      </stop>
      <stop offset="100%" stop-color="{bg_end}">
        <animate attributeName="stop-color" values="{bg_end};{bg_start};{bg_end}" dur="7s" repeatCount="indefinite"/>
      </stop>
    </linearGradient>
    <radialGradient id="halo-{slug}" cx="50%" cy="38%" r="58%">
      <stop offset="0%" stop-color="{accent}" stop-opacity=".70"/>
      <stop offset="100%" stop-color="{glow}" stop-opacity="0"/>
    </radialGradient>
    <filter id="blur-{slug}" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="14"/>
    </filter>
  </defs>
  <rect width="640" height="480" rx="36" fill="url(#bg-{slug})"/>
  <circle cx="320" cy="210" r="154" fill="url(#halo-{slug})" filter="url(#blur-{slug})">
    <animate attributeName="r" values="146;164;146" dur="3.6s" repeatCount="indefinite"/>
  </circle>
  <path d="M58 84c118-62 406-74 522 10" stroke="rgba(255,255,255,.10)" stroke-width="24" fill="none" stroke-linecap="round"/>
  <path d="M84 368c136 64 346 64 472 0" stroke="rgba(255,255,255,.08)" stroke-width="28" fill="none" stroke-linecap="round"/>
  <g opacity=".16">
    <circle cx="114" cy="118" r="14" fill="#fff"/>
    <circle cx="544" cy="120" r="10" fill="#fff"/>
    <circle cx="84" cy="322" r="8" fill="#fff"/>
    <circle cx="572" cy="306" r="12" fill="#fff"/>
  </g>
  {render_center_art(style, variant)}
  <g transform="translate(48 42)">
    <rect x="0" y="0" width="236" height="36" rx="18" fill="rgba(255,255,255,.14)" stroke="rgba(255,255,255,.18)"/>
    <text x="18" y="24" fill="#eef7ff" font-size="15" font-family="Segoe UI, Arial, sans-serif" font-weight="700" letter-spacing="2">{safe_category}</text>
  </g>
  <g transform="translate(48 330)">
    <text x="0" y="0" fill="#ffffff" font-size="38" font-family="Segoe UI, Arial, sans-serif" font-weight="800">{safe_name}</text>
    <text x="0" y="42" fill="rgba(255,255,255,.84)" font-size="18" font-family="Segoe UI, Arial, sans-serif" font-weight="600">{safe_price}</text>
    <text x="0" y="76" fill="rgba(255,255,255,.54)" font-size="13" font-family="Consolas, monospace" font-weight="700" letter-spacing="2">{escape(short_code)}</text>
  </g>
</svg>
"""


def ensure_linea(conn, cod_linea: str, linea: str) -> int:
    existing = conn.execute(
        text(
            """
            select id
            from lineas
            where cod_linea = :cod_linea
               or lower(linea) = lower(:linea)
            order by id
            limit 1
            """
        ),
        {"cod_linea": cod_linea, "linea": linea},
    ).first()
    if existing:
        conn.execute(
            text(
                """
                update lineas
                set cod_linea = :cod_linea,
                    linea = :linea,
                    activo = true
                where id = :id
                """
            ),
            {"id": existing.id, "cod_linea": cod_linea, "linea": linea},
        )
        return int(existing.id)
    created = conn.execute(
        text(
            """
            insert into lineas (cod_linea, linea, activo)
            values (:cod_linea, :linea, true)
            returning id
            """
        ),
        {"cod_linea": cod_linea, "linea": linea},
    ).first()
    return int(created.id)


def ensure_product(
    conn,
    *,
    code: str,
    name: str,
    linea_id: int,
    category: str,
    price_cs: Decimal,
    price_usd: Decimal,
    exchange_rate: Decimal,
    unit_id: int,
    image_url: str,
) -> str:
    normalized_name = name.strip().upper()
    existing = conn.execute(
        text(
            """
            select id
            from productos
            where cod_producto = :code
               or upper(descripcion) = :name
            order by id
            limit 1
            """
        ),
        {"code": code, "name": normalized_name},
    ).first()
    payload = {
        "code": code,
        "name": name,
        "linea_id": linea_id,
        "brand": "La Barrera",
        "price_cs": price_cs,
        "price_usd": price_usd,
        "rate": exchange_rate,
        "reference": category,
        "image_url": image_url,
        "unit_id": unit_id,
        "user_reg": "codex",
        "machine_reg": "seed_barrera_menu",
    }
    if existing:
        conn.execute(
            text(
                """
                update productos
                set cod_producto = :code,
                    descripcion = :name,
                    linea_id = :linea_id,
                    marca = :brand,
                    precio_venta1 = :price_cs,
                    precio_venta2 = :price_cs,
                    precio_venta3 = :price_cs,
                    precio_venta4 = :price_cs,
                    precio_venta5 = :price_cs,
                    precio_venta6 = :price_cs,
                    precio_venta7 = :price_cs,
                    precio_venta1_usd = :price_usd,
                    precio_venta2_usd = :price_usd,
                    precio_venta3_usd = :price_usd,
                    precio_venta4_usd = :price_usd,
                    precio_venta5_usd = :price_usd,
                    precio_venta6_usd = :price_usd,
                    precio_venta7_usd = :price_usd,
                    tasa_cambio = :rate,
                    activo = true,
                    servicio_producto = true,
                    costo_producto = 0,
                    referencia_producto = :reference,
                    image_url = :image_url,
                    tipo_producto = 'DIRECTO',
                    es_por_peso = false,
                    unidad_medida_id = :unit_id,
                    usuario_registro = :user_reg,
                    maquina_registro = :machine_reg
                where id = :id
                """
            ),
            {**payload, "id": int(existing.id)},
        )
        conn.execute(
            text(
                """
                insert into saldos_productos (producto_id, existencia)
                values (:id, 0)
                on conflict (producto_id) do nothing
                """
            ),
            {"id": int(existing.id)},
        )
        return "updated"
    created = conn.execute(
        text(
            """
            insert into productos (
                cod_producto, descripcion, linea_id, marca,
                precio_venta1, precio_venta2, precio_venta3, precio_venta4, precio_venta5, precio_venta6, precio_venta7,
                precio_venta1_usd, precio_venta2_usd, precio_venta3_usd, precio_venta4_usd, precio_venta5_usd, precio_venta6_usd, precio_venta7_usd,
                tasa_cambio, activo, servicio_producto, costo_producto,
                referencia_producto, image_url, tipo_producto, es_por_peso,
                unidad_medida_id, usuario_registro, maquina_registro
            ) values (
                :code, :name, :linea_id, :brand,
                :price_cs, :price_cs, :price_cs, :price_cs, :price_cs, :price_cs, :price_cs,
                :price_usd, :price_usd, :price_usd, :price_usd, :price_usd, :price_usd, :price_usd,
                :rate, true, true, 0,
                :reference, :image_url, 'DIRECTO', false,
                :unit_id, :user_reg, :machine_reg
            )
            returning id
            """
        ),
        payload,
    ).first()
    conn.execute(
        text(
            """
            insert into saldos_productos (producto_id, existencia)
            values (:id, 0)
            """
        ),
        {"id": int(created.id)},
    )
    return "created"


def main() -> None:
    load_env()
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("DATABASE_URL no configurada")
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    engine = create_engine(database_url)
    created_products = 0
    updated_products = 0
    with engine.begin() as conn:
        rate_row = conn.execute(
            text("select rate from exchange_rates order by effective_date desc limit 1")
        ).first()
        exchange_rate = Decimal(str(rate_row.rate if rate_row else "37.0000"))
        unit_row = conn.execute(
            text(
                """
                select id
                from unidades_medida
                where upper(codigo) = 'UNIDAD'
                   or upper(nombre) = 'UNIDAD'
                order by id
                limit 1
                """
            )
        ).first()
        if not unit_row:
            raise SystemExit("No existe unidad de medida UNIDAD")
        unit_id = int(unit_row.id)
        line_map: dict[str, int] = {}
        for cod_linea, linea in LINEAS:
            line_map[linea] = ensure_linea(conn, cod_linea, linea)
        for category, name, price_cs, style in MENU:
            slug = slugify(name)
            image_filename = f"{slug}.svg"
            image_path = ASSET_DIR / image_filename
            image_path.write_text(build_svg(name, category, price_cs, style), encoding="utf-8")
            image_url = f"/static/product_assets/barrera/menu/{image_filename}"
            price_usd = quantize_money(price_cs / exchange_rate) if exchange_rate else Decimal("0.00")
            status = ensure_product(
                conn,
                code=product_code(name),
                name=name,
                linea_id=line_map[category],
                category=category,
                price_cs=price_cs,
                price_usd=price_usd,
                exchange_rate=exchange_rate,
                unit_id=unit_id,
                image_url=image_url,
            )
            if status == "created":
                created_products += 1
            else:
                updated_products += 1
    print(f"Menu La Barrera procesado. Creados: {created_products}. Actualizados: {updated_products}.")
    print(f"Imagenes SVG generadas en: {ASSET_DIR}")


if __name__ == "__main__":
    main()
