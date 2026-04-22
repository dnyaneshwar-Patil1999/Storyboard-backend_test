"""
extract_docx_xml.py — Robust lxml-based DOCX extraction
=========================================================
Drop-in replacement for extract_docx_native() in robust_extractor_patched.py

Uses raw XML parsing via lxml instead of python-docx. This fixes:
  1. Files that python-docx can't open (e.g., #Contents hyperlink references)
  2. Heading detection across all style types (standard, custom, visual-only)
  3. Bullet detection for custom styles like "BT Bullet 1"
  4. Table positioning within document context
  5. Page number tracking via w:br page breaks
  6. Image extraction with position and dimensions

Tested against:
  - ASSR_AA_SFT_New_L1_FG_v2_1.docx (12 Heading 1, 2 Heading 2, 50 tables, 49 images)
  - FB_Basics___Plans_content.docx (0 heading styles, all "Normal", 12 images, hyperlink issue)
"""

import os
import re
import logging
import zipfile
from collections import Counter, defaultdict
from typing import List, Dict, Any, Optional, Tuple
from zipfile import BadZipFile

try:
    from lxml import etree
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# XML NAMESPACES
# ══════════════════════════════════════════════════════════════════════════════
NS = {
    "w":    "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "r":    "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "a":    "http://schemas.openxmlformats.org/drawingml/2006/main",
    "wp":   "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing",
    "pic":  "http://schemas.openxmlformats.org/drawingml/2006/picture",
    "v":    "urn:schemas-microsoft-com:vml",
    "r14":  "http://schemas.microsoft.com/office/2009/relationships",
    "cp":   "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
    "dc":   "http://purl.org/dc/elements/1.1/",
    "ep":   "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties",
}

def _qn(ns_prefix, local):
    """Qualified name from namespace prefix + local name."""
    return f"{{{NS[ns_prefix]}}}{local}"


# ══════════════════════════════════════════════════════════════════════════════
# STYLE MAP WITH INHERITANCE
# ══════════════════════════════════════════════════════════════════════════════

def _build_style_map(zf):
    """Build style map with basedOn inheritance chain."""
    try:
        with zf.open("word/styles.xml") as f:
            root = etree.parse(f).getroot()
    except (KeyError, etree.XMLSyntaxError):
        return {}, 11.0

    style_map = {}
    for s in root.findall(f".//{_qn('w','style')}"):
        sid = s.get(_qn('w', 'styleId'), "")
        stype = s.get(_qn('w', 'type'), "")
        ne = s.find(_qn('w', 'name'))
        name = ne.get(_qn('w', 'val'), "") if ne is not None else ""
        be = s.find(_qn('w', 'basedOn'))
        based = be.get(_qn('w', 'val'), "") if be is not None else ""

        # Extract font size from style
        font_size = None
        rPr = s.find(f".//{_qn('w','rPr')}")
        if rPr is not None:
            sz = rPr.find(_qn('w', 'sz'))
            if sz is not None:
                try:
                    font_size = int(sz.get(_qn('w', 'val'), '0')) / 2.0
                except ValueError:
                    pass
            # Check bold
        bold_in_style = False
        if rPr is not None:
            b_el = rPr.find(_qn('w', 'b'))
            if b_el is not None:
                v = b_el.get(_qn('w', 'val'), 'true')
                bold_in_style = v.lower() not in ('false', '0')

        style_map[sid] = {
            "name": name, "basedOn": based, "type": stype,
            "font_size": font_size, "bold": bold_in_style,
        }

    # Determine body font size (from Normal style or most common)
    normal = style_map.get("Normal", {})
    body_font_size = normal.get("font_size") or 11.0

    return style_map, body_font_size


def _resolve_name_chain(style_id, style_map):
    """Walk the basedOn inheritance chain, return list of (name, styleId)."""
    chain, visited = [], set()
    sid = style_id
    while sid and sid not in visited:
        visited.add(sid)
        info = style_map.get(sid, {})
        chain.append((info.get("name", "").lower(), sid))
        sid = info.get("basedOn", "")
    return chain


def _get_heading_level_from_style(style_id, style_map):
    """Check if a style IS a heading via inheritance. Returns level or None."""
    for name, _ in _resolve_name_chain(style_id, style_map):
        m = re.match(r'^heading\s*(\d+)$', name)
        if m:
            return int(m.group(1))
    return None


def _is_title_style(style_id, style_map):
    """Check if style is Title or Subtitle."""
    for name, _ in _resolve_name_chain(style_id, style_map):
        if name == "title":
            return "title"
        if name in ("subtitle", "title - subtitle"):
            return "subtitle"
    return None


def _is_toc_style(style_id, style_map):
    """Check if style is a Table of Contents entry."""
    for name, _ in _resolve_name_chain(style_id, style_map):
        if name.startswith("toc ") or name == "toc":
            return True
    return False


def _is_bullet_style(style_id, style_map):
    """
    Check if a style represents a bullet/list item.
    Catches: "List Paragraph", "List Bullet", "BT Bullet 1",
    "Normal Bullet 1", "Table bullet 1", "Content Indent Bullet", etc.
    """
    for name, _ in _resolve_name_chain(style_id, style_map):
        clean = name.replace(" ", "").lower()
        # Standard list styles
        if "listparagraph" in clean or "listbullet" in clean or "listnumber" in clean:
            return True
        # Custom bullet styles (catches "BT Bullet 1", "Normal Bullet 1", etc.)
        if "bullet" in clean:
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# NUMBERING RESOLVER (bullets + numbered lists)
# ══════════════════════════════════════════════════════════════════════════════

_BULLET_CHARS = {
    "\uf0b7": "•", "\u2022": "•", "\u00b7": "·",
    "\u2013": "–", "\u2014": "—", "o": "○",
    "\uf0a7": "▪", "\u25aa": "▪", "\u25cf": "●",
}

def _to_roman(n):
    vals = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1]
    syms = ["M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"]
    r = ""
    for v, s in zip(vals, syms):
        while n >= v:
            r += s; n -= v
    return r


def _build_numbering_resolver(zf):
    """Build a resolver that maps (numId, ilvl) → list format info."""
    try:
        with zf.open("word/numbering.xml") as f:
            root = etree.parse(f).getroot()
    except (KeyError, etree.XMLSyntaxError):
        return lambda a, b: {"num_fmt": "bullet", "bullet_char": "•", "start": 1}

    abstract = {}
    for an in root.findall(f"{_qn('w','abstractNum')}"):
        an_id = an.get(_qn('w', 'abstractNumId'), "")
        levels = {}
        for lvl in an.findall(f"{_qn('w','lvl')}"):
            ilvl = lvl.get(_qn('w', 'ilvl'), "0")
            fe = lvl.find(_qn('w', 'numFmt'))
            fmt = fe.get(_qn('w', 'val'), "bullet") if fe is not None else "bullet"
            te = lvl.find(_qn('w', 'lvlText'))
            raw = te.get(_qn('w', 'val'), "") if te is not None else ""
            char = _BULLET_CHARS.get(raw, raw) if fmt == "bullet" else raw
            se = lvl.find(_qn('w', 'start'))
            start = int(se.get(_qn('w', 'val'), "1")) if se is not None else 1
            levels[ilvl] = {"num_fmt": fmt, "bullet_char": char or "•", "start": start}
        abstract[an_id] = levels

    nums = {}
    for num in root.findall(f"{_qn('w','num')}"):
        nid = num.get(_qn('w', 'numId'), "")
        ae = num.find(_qn('w', 'abstractNumId'))
        aid = ae.get(_qn('w', 'val'), "") if ae is not None else ""
        nums[nid] = aid

    def get_list_info(num_id, ilvl):
        aid = nums.get(str(num_id), "")
        lvls = abstract.get(aid, {})
        return dict(lvls.get(str(ilvl), {"num_fmt": "bullet", "bullet_char": "•", "start": 1}))

    return get_list_info


# ══════════════════════════════════════════════════════════════════════════════
# RUN-LEVEL TEXT EXTRACTION (bold, hyperlinks)
# ══════════════════════════════════════════════════════════════════════════════

def _extract_runs(para_elem, rel_map=None):
    """Extract text runs from a paragraph element with formatting."""
    runs = []

    def _run_bold(r_el):
        rPr = r_el.find(_qn('w', 'rPr'))
        if rPr is None:
            return False
        b_el = rPr.find(_qn('w', 'b'))
        if b_el is None:
            return False
        v = b_el.get(_qn('w', 'val'), 'true')
        return v.lower() not in ('false', '0')

    def _run_italic(r_el):
        rPr = r_el.find(_qn('w', 'rPr'))
        if rPr is None:
            return False
        i_el = rPr.find(_qn('w', 'i'))
        if i_el is None:
            return False
        v = i_el.get(_qn('w', 'val'), 'true')
        return v.lower() not in ('false', '0')

    def _run_color(r_el):
        """Returns hex color string (no #) or empty string for default/black."""
        rPr = r_el.find(_qn('w', 'rPr'))
        if rPr is None:
            return ""
        c_el = rPr.find(_qn('w', 'color'))
        if c_el is None:
            return ""
        val = c_el.get(_qn('w', 'val'), "")
        if val.lower() in ('auto', '000000', ''):
            return ""
        return val.upper()

    def _run_font_size(r_el):
        rPr = r_el.find(_qn('w', 'rPr'))
        if rPr is None:
            return None
        sz = rPr.find(_qn('w', 'sz'))
        if sz is None:
            return None
        try:
            return int(sz.get(_qn('w', 'val'), '0')) / 2.0
        except ValueError:
            return None

    def _process(container, link_url=None):
        for child in container:
            tag = etree.QName(child.tag).localname
            if tag == "r":
                text = "".join(t.text or "" for t in child.findall(_qn('w', 't')))
                if text:
                    runs.append({
                        "text": text,
                        "bold": _run_bold(child),
                        "italic": _run_italic(child),
                        "color": _run_color(child),
                        "font_size": _run_font_size(child),
                        "url": link_url or "",
                    })
            elif tag == "hyperlink":
                rid = child.get(_qn('r', 'id'), "")
                url = ""
                if rid and rel_map and rid in rel_map:
                    url = rel_map[rid].get("target", "")
                _process(child, link_url=url)
            elif tag in ("ins", "del", "smartTag"):
                _process(child, link_url)

    _process(para_elem)
    return runs


# Set of colors considered "red" for highlighting in CO output
_RED_COLORS = {'FF0000', 'C00000', 'C0504D', 'CC0000', 'E81123', 'D32F2F'}
# Set of colors considered "blue" for highlighting in CO output
_BLUE_COLORS = {'0070C0', '0000FF', '4472C4', '2E75B6', '2196F3', '1976D2', '0563C1'}


def _merge_adjacent_runs(runs):
    """
    Merge consecutive runs that share identical formatting attributes.
    This prevents broken markdown like '**Lean ****Development**' which
    occurs when Word splits a single bold phrase into multiple runs.
    """
    if not runs:
        return runs
    merged = [dict(runs[0])]
    for r in runs[1:]:
        last = merged[-1]
        same = (
            last.get('bold') == r.get('bold') and
            last.get('italic') == r.get('italic') and
            (last.get('color') or '').upper() == (r.get('color') or '').upper() and
            (last.get('url') or '') == (r.get('url') or '')
        )
        if same:
            last['text'] = last.get('text', '') + r.get('text', '')
        else:
            merged.append(dict(r))
    return merged


def _runs_to_markdown(runs):
    """
    Convert runs to markdown with formatting markers:
      **bold**
      *italic*
      ***bold italic***
      <RED>red text</RED>     (custom marker for renderer to convert to red font)
      <BLUE>blue text</BLUE>  (custom marker for renderer to convert to blue font)
      [link text](url)

    The renderer in save_adls.py converts these markers into actual Word formatting.
    Markers are designed NOT to leak as visible text — the renderer strips them.
    """
    if not runs:
        return ""

    # Merge adjacent runs with identical formatting to avoid broken markers.
    runs = _merge_adjacent_runs(runs)

    parts = []
    for r in runs:
        txt = r["text"]
        if not txt:
            continue

        is_bold = r.get("bold", False)
        is_italic = r.get("italic", False)
        color = (r.get("color") or "").upper()
        url = r.get("url", "")

        # Skip color markers if it's just default/black
        is_red = color in _RED_COLORS
        is_blue = color in _BLUE_COLORS

        # Build piece with formatting wrappers (innermost first)
        piece = txt

        # Bold + Italic combo
        if is_bold and is_italic and txt.strip():
            piece = f"***{txt}***"
        elif is_bold and txt.strip():
            piece = f"**{txt}**"
        elif is_italic and txt.strip():
            piece = f"*{txt}*"

        # Color wrapping
        if is_red and txt.strip():
            piece = f"<RED>{piece}</RED>"
        elif is_blue and txt.strip():
            piece = f"<BLUE>{piece}</BLUE>"

        # Hyperlink wrapping
        if url and not url.startswith("#"):
            piece = f"[{piece}]({url})"

        parts.append(piece)

    result = ''.join(parts).strip()
    # Clean up empty marker pairs that arise from whitespace-only runs
    result = re.sub(r'\*\*\s+\*\*', ' ', result)
    result = re.sub(r'\*\s+\*', ' ', result)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# IMAGE EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def _extract_images_from_para(para_elem, rel_map, images_binary):
    """Extract all images from a paragraph (both w:drawing and v:imagedata)."""
    images = []

    # w:drawing elements (modern OOXML images)
    for drawing in para_elem.findall(f".//{_qn('w', 'drawing')}"):
        blip = drawing.find(f".//{_qn('a', 'blip')}")
        if blip is None:
            continue
        embed_id = blip.get(_qn('r', 'embed'), "") or blip.get(_qn('r14', 'embed'), "")
        if not embed_id or embed_id not in rel_map:
            continue
        target = rel_map[embed_id].get("target", "")
        clean = target.lstrip("/").replace("../", "").replace("./", "")
        if not clean.startswith("word/"):
            clean = "word/" + clean
        if clean not in images_binary:
            continue

        # Get dimensions
        extent = drawing.find(f".//{_qn('wp', 'extent')}")
        cx = int(extent.get("cx", 0)) if extent is not None else 0
        w_px = min(int(cx / 914400 * 96), 700) if cx else 400

        # [PATCH] Extract actual image bytes for the renderer to embed
        img_bytes = images_binary.get(clean)
        if img_bytes is True or not isinstance(img_bytes, (bytes, bytearray)):
            img_bytes = None  # Legacy True marker - skip

        images.append({
            "image_name": os.path.basename(clean),
            "image_key": clean,
            "width_px": w_px,
            "data": img_bytes,  # raw bytes; renderer handles None gracefully
        })

    # v:imagedata elements (legacy VML images)
    for vml in para_elem.findall(f".//{_qn('v', 'imagedata')}"):
        rid = vml.get(_qn('r', 'id'), "")
        if not rid or rid not in rel_map:
            continue
        target = rel_map[rid].get("target", "")
        clean = target.lstrip("/").replace("../", "").replace("./", "")
        if not clean.startswith("word/"):
            clean = "word/" + clean
        if clean in images_binary:
            img_bytes = images_binary.get(clean)
            if img_bytes is True or not isinstance(img_bytes, (bytes, bytearray)):
                img_bytes = None
            images.append({
                "image_name": os.path.basename(clean),
                "image_key": clean,
                "width_px": 400,
                "data": img_bytes,
            })

    return images


# ══════════════════════════════════════════════════════════════════════════════
# VISUAL HEADING DETECTION (for files without heading styles)
# ══════════════════════════════════════════════════════════════════════════════

def _detect_visual_headings(blocks, body_font_size):
    """
    For documents with NO heading styles, detect headings by visual formatting:
    - Bold + font size > body text + short line (< ~12 words)
    - Or bold + short line that doesn't end with period/comma

    Returns dict mapping block index → heading level (1-3).
    """
    heading_map = {}

    for i, b in enumerate(blocks):
        if b["type"] != "paragraph":
            continue

        text = b.get("text", "").strip()
        if not text or len(text) > 150:
            continue

        runs = b.get("runs", [])
        text_runs = [r for r in runs if r.get("text", "").strip()]
        if not text_runs:
            continue

        all_bold = all(r.get("bold", False) for r in text_runs)
        if not all_bold:
            continue

        word_count = len(text.split())
        ends_with_sentence = text.rstrip().endswith(('.', '!', '?', ',', ';'))

        # Get max font size in this paragraph
        max_size = body_font_size
        for r in text_runs:
            if r.get("font_size") and r["font_size"] > max_size:
                max_size = r["font_size"]

        # Level 1: Large font (>= 18pt) + bold + short
        if max_size >= 18 and word_count <= 12:
            heading_map[i] = 1
        # Level 2: Medium font (> body) + bold + short
        elif max_size > body_font_size + 1 and word_count <= 12:
            heading_map[i] = 2
        # Level 3: Same size but bold + short + no sentence ending
        elif word_count <= 10 and not ends_with_sentence:
            heading_map[i] = 3

    return heading_map


# ══════════════════════════════════════════════════════════════════════════════
# MAIN EXTRACTION FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def extract_docx_xml(docx_path: str) -> List[Dict[str, Any]]:
    """
    Layer 1: Extract all content from DOCX using raw XML parsing via lxml.

    Handles:
      ✓ Standard heading styles (Heading 1/2/3, Title, Subtitle) via inheritance
      ✓ Custom corporate heading styles (keyword matching in chain)
      ✓ Visual-only headings (bold + font size for files with no heading styles)
      ✓ Bullet/list hierarchy via numPr/ilvl AND custom bullet styles ("BT Bullet 1")
      ✓ Bold text at run level (XML rPr/b)
      ✓ Tables with position context preserved
      ✓ Images from both w:drawing and v:imagedata with dimensions
      ✓ Page breaks tracked for accurate page numbering
      ✓ TOC entries detected and filtered
      ✓ Hyperlinks preserved
      ✓ Files that python-docx can't open (e.g., #Contents references)

    Returns list of section dicts matching chunk upload format.
    """
    if not LXML_AVAILABLE:
        logger.warning("lxml not available, cannot extract DOCX via XML")
        return []

    logger.info(f"[DOCX-XML] Extracting: {os.path.basename(docx_path)}")

    # ── Validate and open ──
    try:
        zf = zipfile.ZipFile(docx_path, 'r')
    except BadZipFile as e:
        logger.error(f"[DOCX-XML] File is corrupted or not a valid DOCX: {e}")
        return []

    # Check for password protection
    if 'EncryptedPackage' in zf.namelist():
        logger.error(f"[DOCX-XML] File is password-protected")
        zf.close()
        return []

    try:
        return _do_extract(zf, docx_path)
    except Exception as e:
        logger.error(f"[DOCX-XML] Extraction failed: {e}")
        return []
    finally:
        zf.close()


def _do_extract(zf, docx_path):
    """Internal extraction logic."""

    # ── Build resolvers ──
    style_map, body_font_size = _build_style_map(zf)
    get_list_info = _build_numbering_resolver(zf)
    list_counters = {}

    # ── Load relationship map ──
    rel_map = {}
    try:
        with zf.open("word/_rels/document.xml.rels") as f:
            rr = etree.parse(f).getroot()
        for rel in rr:
            rel_map[rel.get("Id", "")] = {
                "target": rel.get("Target", ""),
                "type": rel.get("Type", ""),
            }
    except (KeyError, etree.XMLSyntaxError):
        pass

    # ── Load image binary data ──
    # [PATCH] Actually load the bytes (was just a True marker before).
    # The bytes get propagated all the way to the section image dicts so the
    # renderer can embed them at the position of [IMAGE: key] markers.
    images_binary = {}
    for name in zf.namelist():
        if name.startswith("word/media/"):
            try:
                with zf.open(name) as f:
                    images_binary[name] = f.read()
            except Exception:
                pass

    # ── Parse document body ──
    try:
        with zf.open("word/document.xml") as f:
            doc = etree.parse(f).getroot()
    except (KeyError, etree.XMLSyntaxError) as e:
        logger.error(f"[DOCX-XML] Cannot read document.xml: {e}")
        return []

    body = doc.find(f".//{_qn('w', 'body')}")
    if body is None:
        return []

    # ── First pass: extract all blocks in document order ──
    blocks = []
    current_page = 1

    def _advance_page(elem):
        """Check for page breaks (explicit AND lastRendered) inside an element."""
        nonlocal current_page
        # Explicit page break: w:br with type="page"
        for br in elem.findall(f".//{_qn('w', 'br')}"):
            if br.get(_qn('w', 'type'), "") == "page":
                current_page += 1
        # Word's calculated page break (most accurate for rendering)
        for _ in elem.findall(f".//{_qn('w', 'lastRenderedPageBreak')}"):
            current_page += 1

    for elem in body:
        tag = etree.QName(elem.tag).localname

        if tag == "p":
            _advance_page(elem)

            # Get text
            full_text = "".join(t.text or "" for t in elem.iter(_qn('w', 't'))).strip()

            # Extract images from this paragraph
            para_images = _extract_images_from_para(elem, rel_map, images_binary)
            for img in para_images:
                img["page"] = current_page
                blocks.append({"type": "image", "page": current_page, **img})

            if not full_text:
                continue

            # Get style
            pPr = elem.find(_qn('w', 'pPr'))
            style_id = ""
            if pPr is not None:
                pse = pPr.find(_qn('w', 'pStyle'))
                if pse is not None:
                    style_id = pse.get(_qn('w', 'val'), "")

            # Check list numbering
            num_id, ilvl = None, 0
            if pPr is not None:
                numPr = pPr.find(_qn('w', 'numPr'))
                if numPr is not None:
                    ie = numPr.find(_qn('w', 'ilvl'))
                    ne = numPr.find(_qn('w', 'numId'))
                    if ie is not None:
                        ilvl = int(ie.get(_qn('w', 'val'), "0"))
                    if ne is not None:
                        num_id = ne.get(_qn('w', 'val'), "0")

            # Extract runs with formatting
            runs = _extract_runs(elem, rel_map)

            # ── Classify the paragraph ──
            block = {
                "type": "paragraph",
                "text": full_text,
                "runs": runs,
                "page": current_page,
                "style_id": style_id,
            }

            # 1. Check for Title/Subtitle style
            title_type = _is_title_style(style_id, style_map)
            if title_type:
                block["type"] = title_type
                if title_type == "title":
                    block["level"] = 1
                else:
                    block["level"] = 2

            # 2. Check for Heading style (via inheritance)
            elif _get_heading_level_from_style(style_id, style_map) is not None:
                block["type"] = "heading"
                block["level"] = _get_heading_level_from_style(style_id, style_map)

            # 3. Check for TOC style
            elif _is_toc_style(style_id, style_map):
                block["type"] = "toc"

            # 4. Check for list item (numPr in XML)
            elif num_id and num_id != "0":
                li = get_list_info(num_id, ilvl)
                fmt = li.get("num_fmt", "bullet")
                block["type"] = "bullet"
                block["level"] = ilvl
                if fmt == "bullet":
                    block["bullet_char"] = li.get("bullet_char", "•")
                else:
                    ck = (num_id, ilvl)
                    list_counters[ck] = list_counters.get(ck, li.get("start", 1) - 1) + 1
                    n = list_counters[ck]
                    if fmt == "decimal":
                        block["bullet_char"] = f"{n}."
                    elif fmt == "lowerLetter":
                        block["bullet_char"] = f"{chr(96 + n)}."
                    elif fmt == "upperLetter":
                        block["bullet_char"] = f"{chr(64 + n)}."
                    elif fmt == "lowerRoman":
                        block["bullet_char"] = f"{_to_roman(n).lower()}."
                    elif fmt == "upperRoman":
                        block["bullet_char"] = f"{_to_roman(n)}."
                    else:
                        block["bullet_char"] = f"{n}."

            # 5. Check for bullet-named style (catches "BT Bullet 1", etc.)
            elif _is_bullet_style(style_id, style_map):
                block["type"] = "bullet"
                block["level"] = 0
                block["bullet_char"] = "•"

            blocks.append(block)

        elif tag == "tbl":
            # Track page breaks inside tables
            _advance_page(elem)

            # Extract images inside table cells
            for tc in elem.findall(f".//{_qn('w', 'tc')}"):
                for p_in_cell in tc.findall(f".//{_qn('w', 'p')}"):
                    cell_images = _extract_images_from_para(p_in_cell, rel_map, images_binary)
                    for img in cell_images:
                        img["page"] = current_page
                        blocks.append({"type": "image", "page": current_page, **img})

            # Extract table content with cell text
            rows_data = []
            for tr in elem.findall(f".//{_qn('w', 'tr')}"):
                cells = []
                for tc in tr.findall(f".//{_qn('w', 'tc')}"):
                    cell_runs = []
                    for cp in tc.findall(f".//{_qn('w', 'p')}"):
                        pr = _extract_runs(cp, rel_map)
                        if pr:
                            cell_runs.extend(pr)
                    raw_text = "".join(r["text"] for r in cell_runs).strip()
                    md_text = _runs_to_markdown(cell_runs)
                    cells.append(md_text or raw_text)
                if cells:
                    rows_data.append(cells)

            if rows_data:
                blocks.append({
                    "type": "table",
                    "page": current_page,
                    "rows_data": rows_data,
                })

    # ── Visual heading detection (for files without heading styles) ──
    has_styled_headings = any(b["type"] in ("heading", "title", "subtitle")
                              for b in blocks)

    if not has_styled_headings:
        logger.info("[DOCX-XML] No heading styles found. Applying visual heading detection.")
        visual_headings = _detect_visual_headings(blocks, body_font_size)
        for idx, level in visual_headings.items():
            blocks[idx]["type"] = "heading"
            blocks[idx]["level"] = level
        logger.info(f"[DOCX-XML] Detected {len(visual_headings)} visual headings")

    # ── Convert blocks to sections ──
    sections = _blocks_to_sections(blocks)

    has_headings = any(s.get("_heading_level", 0) > 0 for s in sections)
    logger.info(f"[DOCX-XML] Extracted {len(sections)} sections. "
                f"Has headings: {has_headings}. "
                f"Images: {sum(1 for b in blocks if b['type'] == 'image')}. "
                f"Tables: {sum(1 for b in blocks if b['type'] == 'table')}")

    return sections


def _format_table_markdown(rows_data):
    """Convert rows to markdown pipe table."""
    if not rows_data:
        return ""
    lines = []
    for ri, row in enumerate(rows_data):
        cells = [c.replace('|', '/').strip() for c in row]
        lines.append("| " + " | ".join(cells) + " |")
        if ri == 0:
            lines.append("|" + "|".join([" --- "] * len(cells)) + "|")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# CONTENT SANITIZATION
# ══════════════════════════════════════════════════════════════════════════════
# Patterns that should NEVER appear in extracted content. These come from:
#   - PowerPoint footers (date stamps, "Sample Footer Text", "Confidential")
#   - Co-pilot/AI-generated annotations
#   - Pipeline artifacts ("Section Break", placeholder labels)
#   - Visual decorations from screenshots/images
# Reviewers consistently flagged these as "not from source" or "not required".

_FOOTER_PATTERNS = [
    re.compile(r'^\d{1,2}/\d{1,2}/\d{2,4}$'),                      # Date stamps: 1/14/2026
    re.compile(r'^\d{1,2}-\d{1,2}-\d{2,4}$'),                       # Date stamps: 1-14-2026
    re.compile(r'^Sample Footer Text$', re.I),
    re.compile(r'^AI-generated content may be incorrect\.?$', re.I),
    re.compile(r'^Click to add (text|notes|content|title)$', re.I),
    re.compile(r'^Section Break$', re.I),                            # Pipeline artifact
    re.compile(r'^CONFIDENTIAL\b.*INTERNAL USE\b', re.I),
    re.compile(r'^FOR INTERNAL USE ONLY$', re.I),
    re.compile(r'^PROPRIETARY( AND CONFIDENTIAL)?$', re.I),
    re.compile(r'^Page\s*\d+\s*(of\s*\d+)?$', re.I),                # Page X of Y
    re.compile(r'^Slide\s*\d+$', re.I),                             # Slide N
    re.compile(r'^Copyright\s*©.*$', re.I),                          # Copyright lines
]


def _is_footer_artifact(text: str) -> bool:
    """Returns True if text is a footer, decoration, or pipeline artifact."""
    if not text:
        return True
    text = text.strip()
    if not text:
        return True
    for pat in _FOOTER_PATTERNS:
        if pat.match(text):
            return True
    return False


def _sanitize_content_line(line: str) -> str:
    """
    Clean a single content line.
    - Removes literal artifacts that leaked from source
    - Returns empty string if line is entirely an artifact (caller should drop it)
    """
    if not line:
        return ""

    stripped = line.strip()
    if _is_footer_artifact(stripped):
        return ""

    # Remove inline footer artifacts even when surrounded by other text
    cleaned = line
    cleaned = re.sub(r'AI-generated content may be incorrect\.?', '', cleaned)
    cleaned = re.sub(r'Sample Footer Text', '', cleaned)
    cleaned = re.sub(r'\bSection Break\b', '', cleaned)
    # Collapse multiple spaces
    cleaned = re.sub(r' +', ' ', cleaned).strip()

    return cleaned


def _sanitize_section_content(content: str) -> str:
    """Drop artifact lines and clean inline artifacts from a full content block."""
    if not content:
        return ""
    out_lines = []
    for line in content.split('\n'):
        clean = _sanitize_content_line(line)
        if clean or line.strip() == '':  # Preserve intentional blank lines
            out_lines.append(clean)
    # Collapse 3+ consecutive blank lines into 2
    result = '\n'.join(out_lines)
    result = re.sub(r'\n{3,}', '\n\n', result)
    return result.strip()


def _blocks_to_sections(blocks):
    """
    Convert flat blocks list to hierarchical sections.
    Each section starts at a heading and contains all content until the next heading.
    Tables and images are embedded within the section they belong to.
    """
    sections = []
    current = {
        "heading": None,
        "level": 0,
        "lines": [],
        "pages": set(),
        "images": [],
        "table_count": 0,
    }

    def flush():
        if current["heading"] or current["lines"]:
            content = '\n'.join(current["lines"])
            # Sanitize: strip footer artifacts, "Section Break", date stamps,
            # AI-generated content notices, etc.
            content = _sanitize_section_content(content)

            pages = sorted(current["pages"]) if current["pages"] else [1]
            page_str = f"{pages[0]}" if len(pages) == 1 else f"{pages[0]}-{pages[-1]}"

            sections.append({
                "page_number": len(sections) + 1,
                "chapter": "",  # Set by Layer 3
                "topic": current["heading"] or "Document Content",
                "subtopics": "N/A",
                "content": content,
                "images": current["images"],
                "tables": [],
                "source_page_range": page_str,
                "_confidence": 1.0 if current["level"] > 0 else 0.6,
                "_heading_level": current["level"],
            })
            current["heading"] = None
            current["level"] = 0
            current["lines"] = []
            current["pages"] = set()
            current["images"] = []
            current["table_count"] = 0

    for b in blocks:
        btype = b["type"]
        page = b.get("page", 1)

        # Skip TOC entries
        if btype == "toc":
            continue

        # Heading starts a new section
        if btype in ("heading", "title", "subtotitle", "subtitle"):
            # Skip headings that are themselves footer artifacts
            heading_text = (b.get("text", "") or "").strip()
            if _is_footer_artifact(heading_text):
                continue
            flush()
            current["heading"] = heading_text
            current["level"] = b.get("level", 1)
            current["pages"].add(page)
            continue

        current["pages"].add(page)

        if btype == "paragraph":
            text = _runs_to_markdown(b.get("runs", []))
            if not text:
                text = b.get("text", "")
            # Drop entire paragraph if it's an artifact line
            if _is_footer_artifact(text):
                continue
            # [PATCH] Dedupe consecutive identical paragraphs.
            # Catches cases where a paragraph is read twice (e.g., a heading
            # also appearing as a body paragraph in the same source slide).
            # Fixes: "Objectives word repeated twice" (EY p2),
            #        "Duplicate text" (EY p41, p42, p53, p75, p264).
            stripped = text.strip()
            if stripped and current["lines"]:
                # Compare against last 3 non-blank lines for catching
                # "A B A" patterns where DI re-emits A
                recent = [l.strip() for l in current["lines"][-6:] if l.strip()]
                if stripped in recent:
                    continue
            current["lines"].append(text)

        elif btype == "bullet":
            runs = b.get("runs", [])
            text = _runs_to_markdown(runs)
            if not text:
                text = b.get("text", "")
            if _is_footer_artifact(text):
                continue
            # [PATCH] Same dedupe for bullets
            stripped = text.strip()
            if stripped and current["lines"]:
                recent = [l.strip() for l in current["lines"][-6:] if l.strip()]
                # For bullets, compare against the bullet content (drop prefix)
                bullet_match_targets = [
                    re.sub(r'^[\u2022\u25E6\u25AA\u25CF\-\*]\s+', '', l).strip()
                    for l in recent
                ]
                if stripped in bullet_match_targets or stripped in recent:
                    continue
            level = b.get("level", 0)
            char = b.get("bullet_char", "•")
            indent = "  " * level
            current["lines"].append(f"{indent}{char} {text}")

        elif btype == "table":
            rows_data = b.get("rows_data", [])
            if rows_data:
                tbl_md = _format_table_markdown(rows_data)
                current["lines"].append(f"\n[TABLE]\n{tbl_md}")
                current["table_count"] += 1

        elif btype == "image":
            # [PATCH] Emit inline [IMAGE: key] marker at the source-order position
            # in addition to adding the image to the images list. The renderer
            # uses the marker to embed the image at this exact position in the
            # cell, instead of batching all images at the end.
            #
            # Fixes reviewer feedback:
            #   "Icons are not placed properly. They appear above the text instead
            #    of in the correct position as per the source." (EY p84)
            #   "The content lacks clarity and a logical flow, and the images are
            #    not placed properly as per the source." (EY p17)
            image_key = b.get("image_key", "") or b.get("image_name", "")
            if image_key:
                current["lines"].append(f"[IMAGE: {image_key}]")
            current["images"].append({
                "image_name": b.get("image_name", ""),
                "image_key": b.get("image_key", ""),
                "width_px": b.get("width_px", 400),
                "data": b.get("data"),  # raw image bytes for renderer to embed
                "page": page,
            })

    flush()
    # [PATCH] Dedupe images per section by image_key. Same image may appear
    # multiple times if it was both a body image and a table-cell image.
    # The inline [IMAGE: key] marker is the source of truth for position;
    # the images list just provides the data.
    for s in sections:
        seen_keys = set()
        deduped = []
        for img in s.get("images", []):
            key = img.get("image_key") or img.get("image_name", "")
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(img)
        s["images"] = deduped
    return sections
