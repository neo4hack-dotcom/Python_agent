"""
Web Navigation Tools — Recherche internet, navigation, formulaires et extraction.

Stratégie d'implémentation :
  1. Requêtes HTTP simples : urllib (stdlib, toujours disponible)
  2. Parsing HTML         : html.parser (stdlib)
  3. Navigation avancée  : playwright (optionnel — formulaires JS, screenshots)

Installation playwright (optionnel) :
  pip install playwright
  playwright install chromium
"""

import json
import re
import ssl
import time
import urllib.request
import urllib.parse
import urllib.error
from html.parser import HTMLParser
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.memory import MemoryManager

# Playwright (optionnel)
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


# =========================================================================== #
#  Utilitaires HTML                                                             #
# =========================================================================== #

class _TextExtractor(HTMLParser):
    """Extrait le texte visible d'une page HTML."""

    _SKIP_TAGS = {"script", "style", "head", "meta", "link", "noscript", "nav", "footer"}

    def __init__(self):
        super().__init__()
        self._texts: List[str] = []
        self._tag_stack: List[str] = []

    def handle_starttag(self, tag, attrs):
        self._tag_stack.append(tag.lower())

    def handle_endtag(self, tag):
        t = tag.lower()
        if self._tag_stack and self._tag_stack[-1] == t:
            self._tag_stack.pop()

    def handle_data(self, data):
        if not any(t in self._SKIP_TAGS for t in self._tag_stack):
            text = data.strip()
            if text:
                self._texts.append(text)

    def get_text(self) -> str:
        raw = "\n".join(self._texts)
        return re.sub(r"\n{3,}", "\n\n", raw)


class _LinkExtractor(HTMLParser):
    """Extrait les hyperliens d'une page HTML."""

    def __init__(self, base_url: str = ""):
        super().__init__()
        self.links: List[Dict[str, str]] = []
        self._base_url = base_url

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        attrs_d = dict(attrs)
        href = attrs_d.get("href", "").strip()
        text = attrs_d.get("title", "").strip()
        if not href or href.startswith(("#", "javascript:")):
            return
        href = self._make_absolute(href)
        if href:
            self.links.append({"url": href, "text": text})

    def _make_absolute(self, href: str) -> str:
        if href.startswith("http"):
            return href
        if href.startswith("//"):
            scheme = urllib.parse.urlparse(self._base_url).scheme or "https"
            return f"{scheme}:{href}"
        if href.startswith("/") and self._base_url:
            parsed = urllib.parse.urlparse(self._base_url)
            return f"{parsed.scheme}://{parsed.netloc}{href}"
        if self._base_url:
            return urllib.parse.urljoin(self._base_url, href)
        return href


_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}


def _make_ssl_context(verify: bool = True) -> Optional[ssl.SSLContext]:
    """Crée un contexte SSL. Si verify=False, désactive la vérification des certificats."""
    if verify:
        return None  # urllib utilisera le contexte SSL par défaut (vérification activée)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _fetch_url(
    url: str,
    timeout: int = 20,
    extra_headers: Optional[Dict] = None,
    verify_ssl: bool = True,
    retry_http_fallback: bool = True,
) -> tuple:
    """
    Télécharge une URL. Retourne (html, final_url, status_code).

    Stratégie SSL robuste :
      1. Tentative normale avec vérification SSL
      2. Si erreur SSL → retry automatique sans vérification (verify_ssl=False)
      3. Si toujours en échec et retry_http_fallback=True → fallback HTTP
    """
    headers = dict(_DEFAULT_HEADERS)
    if extra_headers:
        headers.update(extra_headers)

    def _do_request(target_url: str, ssl_ctx) -> tuple:
        req = urllib.request.Request(target_url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            raw = resp.read()
            content_type = resp.headers.get("Content-Type", "")
            charset = "utf-8"
            if "charset=" in content_type:
                charset = content_type.split("charset=")[-1].split(";")[0].strip()
            try:
                html = raw.decode(charset, errors="replace")
            except LookupError:
                html = raw.decode("utf-8", errors="replace")
            return html, resp.url, resp.status

    ssl_ctx = _make_ssl_context(verify=verify_ssl)

    try:
        return _do_request(url, ssl_ctx)

    except (ssl.SSLError, ssl.CertificateError) as e:
        # Retry sans vérification SSL
        if verify_ssl:
            permissive_ctx = _make_ssl_context(verify=False)
            try:
                return _do_request(url, permissive_ctx)
            except Exception:
                pass
        # Fallback HTTP si HTTPS échoue
        if retry_http_fallback and url.startswith("https://"):
            http_url = "http://" + url[8:]
            try:
                return _do_request(http_url, None)
            except Exception:
                pass
        return f"SSL Error: {e}", url, 0

    except urllib.error.URLError as e:
        reason_str = str(e.reason).lower()
        # URLError peut envelopper une erreur SSL
        if any(k in reason_str for k in ("ssl", "certificate", "cert", "handshake", "unknown protocol")):
            if verify_ssl:
                permissive_ctx = _make_ssl_context(verify=False)
                try:
                    return _do_request(url, permissive_ctx)
                except Exception:
                    pass
            if retry_http_fallback and url.startswith("https://"):
                http_url = "http://" + url[8:]
                try:
                    return _do_request(http_url, None)
                except Exception:
                    pass
        return f"URL Error: {e.reason}", url, 0

    except urllib.error.HTTPError as e:
        return f"HTTP Error {e.code}: {e.reason}", url, e.code

    except Exception as e:
        return f"Error: {e}", url, 0


def _html_to_text(html: str, max_chars: int = 8000) -> str:
    """Convertit du HTML en texte lisible."""
    parser = _TextExtractor()
    try:
        parser.feed(html)
        return parser.get_text()[:max_chars]
    except Exception:
        # Fallback brut
        text = re.sub(r"<[^>]+>", " ", html)
        return re.sub(r"\s+", " ", text)[:max_chars]


# =========================================================================== #
#  Définitions des outils                                                       #
# =========================================================================== #

WEB_TOOL_DEFINITIONS: List[Dict] = [
    {
        "name": "web_search",
        "description": (
            "Recherche sur internet via DuckDuckGo. Retourne une liste de résultats "
            "avec titre, URL et extrait. Utiliser pour toute recherche d'informations en ligne."
        ),
        "params": {
            "query":       "La requête de recherche",
            "max_results": "Nombre max de résultats (défaut 10)",
            "region":      "Code région ex 'fr-fr', 'us-en' (défaut 'fr-fr')",
        },
        "required": ["query"],
    },
    {
        "name": "web_navigate",
        "description": (
            "Visite une URL et récupère le contenu textuel de la page. "
            "Utiliser pour lire une page web et en extraire les informations."
        ),
        "params": {
            "url":       "URL complète (doit commencer par http:// ou https://)",
            "max_chars": "Nombre max de caractères à retourner (défaut 8000)",
        },
        "required": ["url"],
    },
    {
        "name": "web_get_links",
        "description": (
            "Extrait tous les hyperliens d'une page web. "
            "Retourne une liste de {url, text}. Utile pour explorer un site."
        ),
        "params": {
            "url":            "URL de la page",
            "filter_pattern": "Regex optionnel pour filtrer les liens (ex: 'article|blog')",
        },
        "required": ["url"],
    },
    {
        "name": "web_extract_structured",
        "description": (
            "Extrait des données structurées d'une page : tableaux HTML, listes, "
            "métadonnées (title, description, og:*). Idéal pour scraper des données tabulaires."
        ),
        "params": {
            "url":         "URL de la page",
            "target_type": "Ce qu'on veut extraire : 'tables', 'lists', 'metadata', 'all' (défaut 'all')",
        },
        "required": ["url"],
    },
    {
        "name": "web_fill_form",
        "description": (
            "Remplit et soumet un formulaire HTML via un navigateur headless (playwright). "
            "Nécessite playwright. Utiliser pour les formulaires de login, de recherche, de contact, etc."
        ),
        "params": {
            "url":             "URL de la page contenant le formulaire",
            "fields":          "Dict {sélecteur_CSS: valeur} à remplir (ex: {'#email': 'user@example.com'})",
            "submit_selector": "Sélecteur CSS du bouton de soumission (défaut 'button[type=submit]')",
            "wait_for":        "Sélecteur CSS ou URL à attendre après soumission (optionnel)",
        },
        "required": ["url", "fields"],
    },
    {
        "name": "web_click",
        "description": (
            "Clique sur un élément d'une page web via navigateur headless (playwright). "
            "Utile pour naviguer dans des interfaces dynamiques."
        ),
        "params": {
            "url":      "URL de la page",
            "selector": "Sélecteur CSS de l'élément à cliquer",
            "wait_for": "Sélecteur CSS ou texte à attendre après le clic (optionnel)",
        },
        "required": ["url", "selector"],
    },
    {
        "name": "web_screenshot",
        "description": (
            "Capture une capture d'écran d'une page web (playwright requis). "
            "Retourne le chemin du fichier enregistré."
        ),
        "params": {
            "url":  "URL de la page",
            "path": "Chemin du fichier de sortie (défaut /tmp/screenshot.png)",
        },
        "required": ["url"],
    },
    {
        "name": "web_download",
        "description": "Télécharge un fichier depuis une URL vers un chemin local.",
        "params": {
            "url":       "URL du fichier à télécharger",
            "dest_path": "Chemin local de destination",
        },
        "required": ["url", "dest_path"],
    },
    {
        "name": "store_finding",
        "description": "Enregistre un résultat important en mémoire pour référence ultérieure.",
        "params": {
            "key":        "Identifiant court",
            "value":      "Contenu du résultat",
            "category":   "Catégorie : 'result', 'link', 'data', 'finding'",
            "confidence": "Score de confiance 0.0-1.0 (défaut 1.0)",
        },
        "required": ["key", "value"],
    },
    {
        "name": "recall_facts",
        "description": "Récupère les faits précédemment enregistrés en mémoire.",
        "params": {
            "category": "Filtrer par catégorie (optionnel)",
        },
        "required": [],
    },
    {
        "name": "think",
        "description": "Étape de raisonnement pur — planifier l'approche avant d'agir.",
        "params": {
            "reasoning": "Votre raisonnement détaillé",
        },
        "required": ["reasoning"],
    },
    {
        "name": "final_answer",
        "description": "Retourne la réponse finale quand la recherche est terminée.",
        "params": {
            "answer":  "La réponse complète avec toutes les informations trouvées",
            "summary": "Résumé en une phrase",
        },
        "required": ["answer"],
    },
]

WEB_TOOL_NAMES = {t["name"] for t in WEB_TOOL_DEFINITIONS}


# =========================================================================== #
#  Exécuteur des outils web                                                    #
# =========================================================================== #

class WebToolExecutor:
    """
    Exécute les outils de navigation web.

    - urllib + html.parser pour les requêtes HTTP simples (stdlib, toujours dispo)
    - playwright pour la navigation avancée (formulaires JS, screenshots) si installé
    """

    def __init__(
        self,
        memory: "MemoryManager",
        dispatch_callback: Optional[Callable] = None,
        timeout: int = 20,
        results_dir: str = "./results",
        verify_ssl: bool = True,
        retry_http_fallback: bool = True,
    ):
        self.memory               = memory
        self._dispatch            = dispatch_callback
        self.timeout              = timeout
        self.results_dir          = results_dir
        self.verify_ssl           = verify_ssl
        self.retry_http_fallback  = retry_http_fallback

        # État playwright (initialisation paresseuse)
        self._playwright  = None
        self._browser     = None
        self._page        = None

    def execute(self, tool_name: str, params: Dict[str, Any]) -> Any:
        if tool_name not in WEB_TOOL_NAMES:
            raise ValueError(f"Outil web inconnu : {tool_name}")
        method = getattr(self, f"_tool_{tool_name}", None)
        if method is None:
            raise ValueError(f"Outil '{tool_name}' non implémenté")
        return method(**params)

    # ------------------------------------------------------------------ #
    #  Recherche et navigation                                             #
    # ------------------------------------------------------------------ #

    def _tool_web_search(
        self,
        query: str,
        max_results: int = 10,
        region: str = "fr-fr",
    ) -> List[Dict[str, str]]:
        """Recherche DuckDuckGo — retourne une liste de résultats."""
        encoded = urllib.parse.quote_plus(query)

        # Tentative 1 : DuckDuckGo HTML standard
        url = f"https://html.duckduckgo.com/html/?q={encoded}&kl={region}"
        html, _, status = _fetch_url(
            url, timeout=self.timeout,
            verify_ssl=self.verify_ssl, retry_http_fallback=self.retry_http_fallback,
        )

        results = self._parse_ddg_results(html, max_results)

        # Tentative 2 : DuckDuckGo Lite si l'analyse a échoué
        if not results:
            url = f"https://lite.duckduckgo.com/lite/?q={encoded}"
            html, _, _ = _fetch_url(
                url, timeout=self.timeout,
                verify_ssl=self.verify_ssl, retry_http_fallback=self.retry_http_fallback,
            )
            results = self._parse_ddg_lite_results(html, max_results)

        if not results:
            return [{
                "info": (
                    f"Recherche effectuée pour '{query}'. "
                    "Résultats non parsables — essayez web_navigate sur une URL spécifique."
                ),
                "raw_length": len(html),
            }]

        return results

    def _parse_ddg_results(self, html: str, max_results: int) -> List[Dict]:
        results = []
        # Liens résultats
        link_matches = re.findall(
            r'<a[^>]+class="[^"]*result__a[^"]*"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
            html, re.DOTALL
        )
        snippets = re.findall(
            r'<a[^>]+class="[^"]*result__snippet[^"]*"[^>]*>(.*?)</a>',
            html, re.DOTALL
        )
        for i, (link, title) in enumerate(link_matches[:max_results]):
            title_clean = re.sub(r"<[^>]+>", "", title).strip()
            snippet = re.sub(r"<[^>]+>", "", snippets[i]).strip() if i < len(snippets) else ""
            # Décodage redirect DDG
            if "/l/?uddg=" in link or "uddg=" in link:
                try:
                    link = urllib.parse.unquote(link.split("uddg=")[1].split("&")[0])
                except Exception:
                    pass
            results.append({
                "rank": i + 1,
                "title": title_clean,
                "url": link,
                "snippet": snippet,
            })
        return results

    def _parse_ddg_lite_results(self, html: str, max_results: int) -> List[Dict]:
        results = []
        # DDG Lite : tableaux de résultats
        rows = re.findall(
            r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE
        )
        for row in rows:
            link_m = re.search(r'href="(https?://[^"]+)"', row)
            text_m = re.search(r'<a[^>]+>(.*?)</a>', row, re.DOTALL)
            if link_m and text_m:
                title = re.sub(r"<[^>]+>", "", text_m.group(1)).strip()
                if title:
                    results.append({
                        "rank": len(results) + 1,
                        "title": title,
                        "url": link_m.group(1),
                        "snippet": "",
                    })
            if len(results) >= max_results:
                break
        return results

    def _tool_web_navigate(
        self,
        url: str,
        max_chars: int = 8000,
    ) -> Dict[str, Any]:
        """Visite une URL et retourne le contenu textuel."""
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        html, final_url, status = _fetch_url(
            url, timeout=self.timeout,
            verify_ssl=self.verify_ssl, retry_http_fallback=self.retry_http_fallback,
        )

        if status == 0 or html.startswith(("HTTP Error", "URL Error", "Error:", "SSL Error")):
            return {"url": url, "status": status, "error": html, "content": ""}

        text = _html_to_text(html, max_chars=max_chars)
        return {
            "url":         final_url,
            "status":      status,
            "text_length": len(text),
            "content":     text,
        }

    def _tool_web_get_links(
        self,
        url: str,
        filter_pattern: str = "",
    ) -> List[Dict[str, str]]:
        """Extrait les liens d'une page web."""
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        html, final_url, _ = _fetch_url(
            url, timeout=self.timeout,
            verify_ssl=self.verify_ssl, retry_http_fallback=self.retry_http_fallback,
        )

        parser = _LinkExtractor(base_url=final_url)
        try:
            parser.feed(html)
        except Exception:
            pass

        links = parser.links
        if filter_pattern:
            try:
                pat = re.compile(filter_pattern, re.IGNORECASE)
                links = [
                    lk for lk in links
                    if pat.search(lk["url"]) or pat.search(lk.get("text", ""))
                ]
            except re.error:
                pass

        # Déduplique par URL
        seen: set = set()
        unique = []
        for lk in links:
            if lk["url"] not in seen:
                seen.add(lk["url"])
                unique.append(lk)

        return unique[:100]

    def _tool_web_extract_structured(
        self,
        url: str,
        target_type: str = "all",
    ) -> Dict[str, Any]:
        """Extrait tableaux, listes et métadonnées d'une page."""
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        html, final_url, status = _fetch_url(
            url, timeout=self.timeout,
            verify_ssl=self.verify_ssl, retry_http_fallback=self.retry_http_fallback,
        )
        result: Dict[str, Any] = {"url": final_url, "status": status}

        if target_type in ("tables", "all"):
            tables = []
            for table_html in re.findall(
                r"<table[^>]*>(.*?)</table>", html, re.DOTALL | re.IGNORECASE
            )[:5]:
                rows = []
                for row_m in re.finditer(
                    r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL | re.IGNORECASE
                ):
                    cells = re.findall(
                        r"<t[dh][^>]*>(.*?)</t[dh]>",
                        row_m.group(1), re.DOTALL | re.IGNORECASE
                    )
                    clean = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
                    if any(clean):
                        rows.append(clean)
                if rows:
                    tables.append(rows)
            result["tables"] = tables

        if target_type in ("metadata", "all"):
            metas: Dict[str, str] = {}
            for m in re.finditer(r"<meta[^>]+>", html, re.IGNORECASE):
                tag = m.group(0)
                name_m = re.search(r'(?:name|property)="([^"]+)"', tag, re.IGNORECASE)
                cont_m = re.search(r'content="([^"]+)"', tag, re.IGNORECASE)
                if name_m and cont_m:
                    metas[name_m.group(1)] = cont_m.group(1)
            title_m = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
            if title_m:
                metas["title"] = title_m.group(1).strip()
            result["metadata"] = metas

        if target_type in ("lists", "all"):
            lists = []
            for list_m in re.finditer(
                r"<[ou]l[^>]*>(.*?)</[ou]l>", html, re.DOTALL | re.IGNORECASE
            ):
                items = re.findall(
                    r"<li[^>]*>(.*?)</li>",
                    list_m.group(1), re.DOTALL | re.IGNORECASE
                )
                clean = [re.sub(r"<[^>]+>", "", i).strip() for i in items if i.strip()]
                if clean:
                    lists.append(clean[:20])
            result["lists"] = lists[:10]

        return result

    # ------------------------------------------------------------------ #
    #  Navigation avancée (playwright)                                     #
    # ------------------------------------------------------------------ #

    def _tool_web_fill_form(
        self,
        url: str,
        fields: Dict[str, str],
        submit_selector: str = "button[type=submit]",
        wait_for: str = "",
    ) -> Dict[str, Any]:
        """Remplit et soumet un formulaire via playwright."""
        if not PLAYWRIGHT_AVAILABLE:
            return {
                "error": (
                    "playwright n'est pas installé. "
                    "Installez-le : pip install playwright && playwright install chromium"
                ),
                "fallback": (
                    "Utilisez web_navigate pour inspecter le HTML du formulaire, "
                    "puis web_fill_form sera disponible après installation."
                ),
                "success": False,
            }
        try:
            self._ensure_browser()
            self._page.goto(url, wait_until="networkidle", timeout=self.timeout * 1000)

            for selector, value in fields.items():
                try:
                    self._page.fill(selector, str(value))
                except Exception:
                    try:
                        self._page.type(selector, str(value))
                    except Exception:
                        pass

            try:
                self._page.click(submit_selector)
            except Exception:
                self._page.keyboard.press("Enter")

            if wait_for:
                try:
                    if wait_for.startswith("http"):
                        self._page.wait_for_url(wait_for, timeout=10_000)
                    else:
                        self._page.wait_for_selector(wait_for, timeout=10_000)
                except Exception:
                    pass

            time.sleep(1)
            text = _html_to_text(self._page.content(), max_chars=5000)
            return {
                "success":   True,
                "final_url": self._page.url,
                "content":   text,
            }
        except Exception as e:
            return {"error": str(e), "success": False}

    def _tool_web_click(
        self,
        url: str,
        selector: str,
        wait_for: str = "",
    ) -> Dict[str, Any]:
        """Clique sur un élément via playwright."""
        if not PLAYWRIGHT_AVAILABLE:
            return {
                "error": "playwright non installé. pip install playwright && playwright install chromium",
                "success": False,
            }
        try:
            self._ensure_browser()
            self._page.goto(url, wait_until="domcontentloaded", timeout=self.timeout * 1000)
            self._page.click(selector)

            if wait_for:
                try:
                    self._page.wait_for_selector(wait_for, timeout=5_000)
                except Exception:
                    pass

            time.sleep(1)
            text = _html_to_text(self._page.content(), max_chars=5000)
            return {
                "success":   True,
                "final_url": self._page.url,
                "content":   text,
            }
        except Exception as e:
            return {"error": str(e), "success": False}

    def _tool_web_screenshot(
        self,
        url: str,
        path: str = "/tmp/screenshot.png",
    ) -> Dict[str, Any]:
        """Capture d'écran d'une page web via playwright."""
        if not PLAYWRIGHT_AVAILABLE:
            return {
                "error": "playwright non installé. pip install playwright && playwright install chromium",
                "success": False,
            }
        import os
        try:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            self._ensure_browser()
            self._page.goto(url, wait_until="networkidle", timeout=self.timeout * 1000)
            self._page.screenshot(path=path, full_page=True)
            return {"success": True, "path": path, "url": self._page.url}
        except Exception as e:
            return {"error": str(e), "success": False}

    def _tool_web_download(
        self,
        url: str,
        dest_path: str,
    ) -> Dict[str, Any]:
        """Télécharge un fichier depuis une URL."""
        import os
        try:
            os.makedirs(
                os.path.dirname(os.path.abspath(dest_path)) or ".",
                exist_ok=True,
            )
            ssl_ctx = _make_ssl_context(verify=self.verify_ssl)
            req = urllib.request.Request(url, headers=dict(_DEFAULT_HEADERS))
            try:
                with urllib.request.urlopen(req, timeout=self.timeout, context=ssl_ctx) as resp:
                    with open(dest_path, "wb") as f:
                        f.write(resp.read())
            except (ssl.SSLError, ssl.CertificateError, urllib.error.URLError) as e:
                # Retry sans vérification SSL
                permissive_ctx = _make_ssl_context(verify=False)
                with urllib.request.urlopen(req, timeout=self.timeout, context=permissive_ctx) as resp:
                    with open(dest_path, "wb") as f:
                        f.write(resp.read())
            size = os.path.getsize(dest_path)
            return {"success": True, "path": dest_path, "size_bytes": size}
        except Exception as e:
            return {"error": str(e), "success": False}

    # ------------------------------------------------------------------ #
    #  Outils mémoire (cohérence avec les autres agents)                  #
    # ------------------------------------------------------------------ #

    def _tool_store_finding(
        self,
        key: str,
        value: Any,
        category: str = "finding",
        confidence: float = 1.0,
    ) -> str:
        self.memory.store_fact(
            key, value, source="web-agent",
            category=category, confidence=confidence,
        )
        return f"Résultat '{key}' enregistré en mémoire."

    def _tool_recall_facts(self, category: Optional[str] = None) -> List[Dict]:
        facts = (
            self.memory.get_facts_by_category(category)
            if category else self.memory.all_facts()
        )
        return [
            {"key": f.key, "value": f.value, "category": f.category}
            for f in facts
        ]

    def _tool_think(self, reasoning: str = "") -> str:
        return f"[RAISONNEMENT] {reasoning}"

    def _tool_final_answer(self, answer: str, summary: str = "") -> Dict[str, str]:
        return {"answer": answer, "summary": summary or answer[:200]}

    # ------------------------------------------------------------------ #
    #  Gestion playwright                                                  #
    # ------------------------------------------------------------------ #

    def _ensure_browser(self):
        """Initialise playwright de manière paresseuse."""
        if self._playwright is None:
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=True)
        if self._page is None or self._page.is_closed():
            self._page = self._browser.new_page()
            self._page.set_default_timeout(self.timeout * 1000)

    def close(self):
        """Libère les ressources playwright."""
        try:
            if self._page and not self._page.is_closed():
                self._page.close()
            if self._browser:
                self._browser.close()
            if self._playwright:
                self._playwright.stop()
        except Exception:
            pass
        finally:
            self._page      = None
            self._browser   = None
            self._playwright = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
