#!/usr/bin/env python3
"""
Federal Register PDF Scraper (v5 - Smart Resumption)

Downloads individual notice-level PDFs from the U.S. Federal Register archive
(https://www.govinfo.gov/app/collection/fr/) from 1936 to present.

This script uses govinfo.gov sitemaps and direct HTTP requests - NO API KEY REQUIRED.
It is fully replicable for any user on any local device.

Features:
- Smart resumption with two-tier completion tracking:
    1. Package-level: tracks individual daily packages (e.g., FR-2024-01-02)
    2. Sitemap-level: tracks whether an entire year's sitemap was fully walked
  Per-package short-circuiting only activates within sitemaps that were previously
  fully walked, preventing data gaps in partially-completed years.
- Parallel PDF downloads via thread pool (configurable workers, default 10)
- Stock and flow approach: Initial run downloads all historical PDFs, subsequent runs
  download only new documents since the last scan
- SQLite manifest for tracking downloads and resumability (thread-safe)
- Nested folder structure: Year/Month/Day/Agency/Type/
- Document type filtering (RULE, PRORULE, NOTICE, etc.)
- Optional inclusion of FrontMatter and ReaderAids
- Dynamic detection of historic vs. modern format (no hardcoded year cutoffs)
- Graceful rate limiting (per-thread delays)
- Full rescan option for comprehensive checks

Usage:
    python federal_register_scraper.py                       # Normal run (10 parallel workers)
    python federal_register_scraper.py --workers 6           # Use 6 parallel download threads
    python federal_register_scraper.py --full-rescan         # Full historical rescan
    python federal_register_scraper.py --types RULE,PRORULE  # Only rules
    python federal_register_scraper.py --include-frontmatter # Include TOC and reader aids
    python federal_register_scraper.py --verify              # Verify manifest against files
    python federal_register_scraper.py --stats               # Show download statistics

Document Types (for --types filter):
    RULE     - Final rules
    PRORULE  - Proposed rules
    NOTICE   - Notices
    PRESDOCU - Presidential documents
    CORRECT  - Corrections
    UNKNOWN  - Uncategorized

Data Sources (no API key required):
- Sitemaps: https://www.govinfo.gov/sitemap/FR_sitemap_index.xml
- MODS metadata: https://www.govinfo.gov/metadata/pkg/{packageId}/mods.xml
- PDFs: https://www.govinfo.gov/content/pkg/{packageId}/pdf/{granuleId}.pdf
"""

import sys
import re
import time
import sqlite3
import hashlib
import argparse
import logging
import gzip
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from xml.etree import ElementTree as ET
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
# CONFIGURATION - Edit these values as needed
# =============================================================================

OUTPUT_DIR = Path("D:/Test_AWS_Dereg/federal_register_pdfs")
MANIFEST_DB = Path("D:/Test_AWS_Dereg/fr_manifest.db")
LOG_FILE = Path("D:/Test_AWS_Dereg/fr_scraper.log")

INDEX_REQUEST_DELAY = 1.5
PDF_DOWNLOAD_DELAY = 0.75
MAX_DOWNLOAD_WORKERS = 10

GOVINFO_BASE = "https://www.govinfo.gov"
SITEMAP_INDEX_URL = f"{GOVINFO_BASE}/sitemap/FR_sitemap_index.xml"
METADATA_URL_TEMPLATE = f"{GOVINFO_BASE}/metadata/pkg/{{package_id}}/mods.xml"
CONTENT_URL_TEMPLATE = f"{GOVINFO_BASE}/content/pkg/{{package_id}}/pdf/{{granule_id}}.pdf"
FULL_ISSUE_URL_TEMPLATE = f"{GOVINFO_BASE}/content/pkg/{{package_id}}/pdf/{{package_id}}.pdf"

LOG_LEVEL = logging.INFO
SITEMAP_NS = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
MODS_NS = {'mods': 'http://www.loc.gov/mods/v3'}

# =============================================================================
# END CONFIGURATION
# =============================================================================


def setup_logging() -> logging.Logger:
    """Configure logging to both file and console."""
    logger = logging.getLogger("fr_scraper")
    logger.setLevel(LOG_LEVEL)
    if logger.handlers:
        return logger
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def normalize_entity_name(name: str) -> str:
    """Convert entity/agency name to machine-readable format."""
    if not name:
        return "unknown_entity"
    normalized = name.lower()
    normalized = normalized.replace(".", "")
    normalized = normalized.replace(" ", "_")
    normalized = re.sub(r'[^a-z0-9_\-]', '', normalized)
    normalized = re.sub(r'_+', '_', normalized)
    normalized = normalized.strip('_')
    return normalized if normalized else "unknown_entity"


def create_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'FederalRegisterResearchScraper/1.0 (Academic/Policy Research; Respectful Crawling)',
        'Accept': 'application/xml, application/pdf, */*',
        'Accept-Encoding': 'gzip, deflate'})
    return session


class ManifestDB:
    """SQLite database for tracking downloaded documents."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = None
        self._lock = threading.Lock()
        self._connect()
        self._create_tables()
    
    def _connect(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
    
    def _create_tables(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                document_id TEXT PRIMARY KEY,
                package_id TEXT NOT NULL,
                granule_id TEXT,
                title TEXT,
                agency TEXT,
                section TEXT,
                publish_date DATE,
                first_page INTEGER,
                last_page INTEGER,
                filepath TEXT,
                file_size INTEGER,
                checksum TEXT,
                downloaded_at TIMESTAMP,
                download_url TEXT,
                is_full_issue BOOLEAN DEFAULT FALSE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS failed_downloads (
                document_id TEXT PRIMARY KEY,
                package_id TEXT NOT NULL,
                download_url TEXT,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                last_attempt TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scan_metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP
            )
        ''')
        
        # Processed sitemaps with fully_walked tracking.
        # fully_walked = True means every package in this sitemap was visited
        # on a prior run. This is the gate for per-package short-circuiting.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_sitemaps (
                sitemap_url TEXT PRIMARY KEY,
                last_modified TEXT,
                processed_at TIMESTAMP,
                fully_walked BOOLEAN DEFAULT FALSE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS completed_packages (
                package_id TEXT PRIMARY KEY,
                granule_count INTEGER NOT NULL,
                completed_at TIMESTAMP NOT NULL
            )
        ''')
        
        # Migration: add fully_walked column to pre-existing processed_sitemaps
        try:
            cursor.execute('ALTER TABLE processed_sitemaps ADD COLUMN fully_walked BOOLEAN DEFAULT FALSE')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_publish_date ON documents(publish_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_package_id ON documents(package_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_agency ON documents(agency)')
        self.conn.commit()
    
    def document_exists(self, document_id: str) -> bool:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('SELECT 1 FROM documents WHERE document_id = ?', (document_id,))
            return cursor.fetchone() is not None
    
    def add_document(self, doc_info: Dict[str, Any]):
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO documents 
            (document_id, package_id, granule_id, title, agency, section, 
             publish_date, first_page, last_page, filepath, file_size, 
             checksum, downloaded_at, download_url, is_full_issue)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            doc_info['document_id'], doc_info['package_id'],
            doc_info.get('granule_id'), doc_info.get('title'),
            doc_info.get('agency'), doc_info.get('section'),
            doc_info.get('publish_date'), doc_info.get('first_page'),
            doc_info.get('last_page'), doc_info.get('filepath'),
            doc_info.get('file_size'), doc_info.get('checksum'),
            datetime.now().isoformat(), doc_info.get('download_url'),
            doc_info.get('is_full_issue', False)
        ))
            self.conn.commit()
    
    def add_failed_download(self, document_id: str, package_id: str, 
                           download_url: str, error_message: str):
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO failed_downloads 
                (document_id, package_id, download_url, error_message, retry_count, last_attempt)
                VALUES (?, ?, ?, ?, 
                        COALESCE((SELECT retry_count + 1 FROM failed_downloads WHERE document_id = ?), 1),
                        ?)
            ''', (document_id, package_id, download_url, error_message, 
                  document_id, datetime.now().isoformat()))
            self.conn.commit()
    
    def remove_failed_download(self, document_id: str):
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM failed_downloads WHERE document_id = ?', (document_id,))
            self.conn.commit()
    
    def get_failed_downloads(self, max_retries: int = 5) -> List[Dict]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT * FROM failed_downloads 
                WHERE retry_count < ? ORDER BY last_attempt ASC
            ''', (max_retries,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_last_scan_date(self) -> Optional[datetime]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('SELECT value FROM scan_metadata WHERE key = ?', ('last_scan_date',))
            row = cursor.fetchone()
            return datetime.fromisoformat(row['value']) if row else None
    
    def set_last_scan_date(self, scan_date: datetime):
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO scan_metadata (key, value, updated_at)
                VALUES (?, ?, ?)
            ''', ('last_scan_date', scan_date.isoformat(), datetime.now().isoformat()))
            self.conn.commit()
    
    def get_sitemap_last_modified(self, sitemap_url: str) -> Optional[str]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('SELECT last_modified FROM processed_sitemaps WHERE sitemap_url = ?', 
                          (sitemap_url,))
            row = cursor.fetchone()
            return row['last_modified'] if row else None
    
    def is_sitemap_fully_walked(self, sitemap_url: str) -> bool:
        """Check whether a sitemap was previously fully walked (every package visited)."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                'SELECT fully_walked FROM processed_sitemaps WHERE sitemap_url = ?',
                (sitemap_url,))
            row = cursor.fetchone()
            return bool(row['fully_walked']) if row else False
    
    def set_sitemap_processed(self, sitemap_url: str, last_modified: str,
                              fully_walked: bool = False):
        """Mark a sitemap as processed, with fully_walked status."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO processed_sitemaps 
                (sitemap_url, last_modified, processed_at, fully_walked)
                VALUES (?, ?, ?, ?)
            ''', (sitemap_url, last_modified, datetime.now().isoformat(), fully_walked))
            self.conn.commit()
    
    def mark_package_complete(self, package_id: str, granule_count: int):
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO completed_packages 
                (package_id, granule_count, completed_at)
                VALUES (?, ?, ?)
            ''', (package_id, granule_count, datetime.now().isoformat()))
            self.conn.commit()
    
    def is_package_complete(self, package_id: str) -> bool:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                'SELECT 1 FROM completed_packages WHERE package_id = ?', (package_id,))
            return cursor.fetchone() is not None
    
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            cursor = self.conn.cursor()
            stats = {}
            cursor.execute('SELECT COUNT(*) as count FROM documents')
            stats['total_documents'] = cursor.fetchone()['count']
            cursor.execute('SELECT COUNT(*) as count FROM documents WHERE is_full_issue = 1')
            stats['full_issues'] = cursor.fetchone()['count']
            stats['individual_granules'] = stats['total_documents'] - stats['full_issues']
            cursor.execute('SELECT SUM(file_size) as total FROM documents')
            result = cursor.fetchone()['total']
            stats['total_size_bytes'] = result if result else 0
            stats['total_size_gb'] = round(stats['total_size_bytes'] / (1024**3), 2)
            cursor.execute('SELECT MIN(publish_date) as min_date, MAX(publish_date) as max_date FROM documents')
            row = cursor.fetchone()
            stats['earliest_date'] = row['min_date']
            stats['latest_date'] = row['max_date']
            cursor.execute('SELECT COUNT(*) as count FROM failed_downloads')
            stats['failed_downloads'] = cursor.fetchone()['count']
            cursor.execute('SELECT COUNT(*) as count FROM completed_packages')
            stats['completed_packages'] = cursor.fetchone()['count']
            cursor.execute('SELECT COUNT(*) as count FROM processed_sitemaps WHERE fully_walked = 1')
            stats['fully_walked_sitemaps'] = cursor.fetchone()['count']
            cursor.execute('SELECT COUNT(*) as count FROM processed_sitemaps')
            stats['total_sitemaps_tracked'] = cursor.fetchone()['count']
            cursor.execute('SELECT value FROM scan_metadata WHERE key = ?', ('last_scan_date',))
            row = cursor.fetchone()
            stats['last_scan_date'] = datetime.fromisoformat(row['value']) if row else None
            return stats
    
    def close(self):
        if self.conn:
            self.conn.close()


class FederalRegisterScraper:
    """Main scraper class for Federal Register PDFs using sitemaps (no API key)."""
    
    CONSECUTIVE_COMPLETE_THRESHOLD = 2
    
    def __init__(self, output_dir: Path, manifest: ManifestDB, 
                 include_frontmatter: bool = False, types_filter: List[str] = None):
        self.output_dir = output_dir
        self.manifest = manifest
        self.session = create_session()
        self.logger = logging.getLogger("fr_scraper")
        self.include_frontmatter = include_frontmatter
        self.types_filter = [t.upper() for t in types_filter] if types_filter else None
        self.download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS)
        self._thread_local = threading.local()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.include_frontmatter:
            self.logger.info("Including FrontMatter and ReaderAids sections")
        if self.types_filter:
            self.logger.info(f"Filtering document types: {', '.join(self.types_filter)}")
    
    def _get_thread_session(self) -> requests.Session:
        if not hasattr(self._thread_local, 'session'):
            self._thread_local.session = create_session()
        return self._thread_local.session
    
    def _fetch_xml(self, url: str) -> Optional[ET.Element]:
        try:
            time.sleep(INDEX_REQUEST_DELAY)
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            content = response.content
            if url.endswith('.gz'):
                content = gzip.decompress(content)
            return ET.fromstring(content)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch XML from {url}: {e}")
            return None
        except ET.ParseError as e:
            self.logger.error(f"Failed to parse XML from {url}: {e}")
            return None
    
    def _download_pdf(self, url: str, filepath: Path) -> Tuple[bool, Optional[str], Optional[int]]:
        """Download a PDF file. Thread-safe. Returns: (success, checksum, file_size)"""
        try:
            time.sleep(PDF_DOWNLOAD_DELAY)
            session = self._get_thread_session()
            response = session.get(url, timeout=120, stream=True)
            response.raise_for_status()
            filepath.parent.mkdir(parents=True, exist_ok=True)
            sha256 = hashlib.sha256()
            file_size = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        sha256.update(chunk)
                        file_size += len(chunk)
            return True, sha256.hexdigest(), file_size
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download {url}: {e}")
            return False, None, None
        except IOError as e:
            self.logger.error(f"Failed to write file {filepath}: {e}")
            return False, None, None
    
    def _get_filepath_for_document(self, publish_date: datetime, agency: str,
                                   document_id: str, doc_type: str = None,
                                   is_full_issue: bool = False) -> Path:
        year = str(publish_date.year)
        month = f"{publish_date.month:02d}"
        day = f"{publish_date.day:02d}"
        if is_full_issue:
            return self.output_dir / year / month / day / "full_issue" / f"{document_id}.pdf"
        entity_folder = normalize_entity_name(agency) if agency else "unknown_entity"
        type_folder = (doc_type or 'unknown').lower()
        safe_doc_id = re.sub(r'[<>:"/\\|?*]', '_', document_id)
        return self.output_dir / year / month / day / entity_folder / type_folder / f"{safe_doc_id}.pdf"
    
    def _extract_year_from_sitemap_url(self, url: str) -> Optional[int]:
        match = re.search(r'FR_(\d{4})_sitemap', url)
        return int(match.group(1)) if match else None
    
    def _parse_date_from_package_id(self, package_id: str) -> Optional[datetime]:
        match = re.search(r'FR-(\d{4})-(\d{2})-(\d{2})', package_id)
        if match:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        return None
    
    def get_sitemap_index(self) -> List[Dict[str, str]]:
        """Fetch the FR sitemap index. Returns list sorted by year DESCENDING."""
        self.logger.info("Fetching sitemap index...")
        root = self._fetch_xml(SITEMAP_INDEX_URL)
        if root is None:
            return []
        sitemaps = []
        for sitemap in root.findall('sm:sitemap', SITEMAP_NS):
            loc = sitemap.find('sm:loc', SITEMAP_NS)
            lastmod = sitemap.find('sm:lastmod', SITEMAP_NS)
            if loc is not None:
                url = loc.text
                year = self._extract_year_from_sitemap_url(url)
                sitemaps.append({
                    'url': url,
                    'lastmod': lastmod.text if lastmod is not None else None,
                    'year': year})
        sitemaps.sort(key=lambda x: x.get('year') or 0, reverse=True)
        self.logger.info(f"Found {len(sitemaps)} yearly sitemaps (sorted newest to oldest: {sitemaps[0]['year']} -> {sitemaps[-1]['year']})")
        return sitemaps
    
    def get_packages_from_sitemap(self, sitemap_url: str) -> List[Dict[str, str]]:
        """Fetch a yearly sitemap. Returns list sorted by date DESCENDING."""
        root = self._fetch_xml(sitemap_url)
        if root is None:
            return []
        packages = []
        for url_elem in root.findall('sm:url', SITEMAP_NS):
            loc = url_elem.find('sm:loc', SITEMAP_NS)
            lastmod = url_elem.find('sm:lastmod', SITEMAP_NS)
            if loc is not None:
                url = loc.text
                match = re.search(r'(FR-\d{4}-\d{2}-\d{2})', url)
                if match:
                    package_id = match.group(1)
                    publish_date = self._parse_date_from_package_id(package_id)
                    packages.append({
                        'url': url,
                        'lastmod': lastmod.text if lastmod is not None else None,
                        'package_id': package_id,
                        'date': publish_date})
        packages.sort(key=lambda x: x.get('date') or datetime.min, reverse=True)
        return packages
    
    def get_mods_metadata(self, package_id: str) -> Optional[ET.Element]:
        url = METADATA_URL_TEMPLATE.format(package_id=package_id)
        return self._fetch_xml(url)
    
    def parse_granules_from_mods(self, mods_root: ET.Element, package_id: str) -> List[Dict]:
        """Parse granule information from MODS metadata."""
        granules = []
        for related in mods_root.findall('.//mods:relatedItem[@type="constituent"]', MODS_NS):
            granule_info = {}
            granule_id = None
            access_id = related.find('mods:extension/mods:accessId', MODS_NS)
            if access_id is not None and access_id.text:
                granule_id = access_id.text
            else:
                identifier = related.find('mods:identifier[@type="FR Doc No."]', MODS_NS)
                if identifier is not None and identifier.text:
                    granule_id = identifier.text
                else:
                    id_attr = related.get('ID', '')
                    if id_attr.startswith('id-'):
                        granule_id = id_attr[3:]
            if not granule_id:
                continue
            granule_info['granule_id'] = granule_id
            is_frontmatter = 'FrontMatter' in granule_id or 'ReaderAids' in granule_id
            if is_frontmatter and not self.include_frontmatter:
                continue
            granule_class = related.find('mods:extension/mods:granuleClass', MODS_NS)
            if granule_class is not None and granule_class.text:
                granule_info['section'] = granule_class.text.upper()
            else:
                genre_elem = related.find('mods:genre', MODS_NS)
                if genre_elem is not None:
                    granule_info['section'] = genre_elem.text.upper() if genre_elem.text else 'UNKNOWN'
                else:
                    granule_info['section'] = 'UNKNOWN'
            if self.types_filter and granule_info['section'] not in self.types_filter:
                continue
            title_elem = related.find('mods:titleInfo/mods:title', MODS_NS)
            if title_elem is not None:
                granule_info['title'] = title_elem.text
            agency = None
            agency_elem = related.find('mods:extension/mods:agency', MODS_NS)
            if agency_elem is not None and agency_elem.text:
                agency = agency_elem.text
            else:
                name_elem = related.find('.//mods:name[@type="corporate"]/mods:namePart', MODS_NS)
                if name_elem is not None:
                    agency = name_elem.text
            granule_info['agency'] = agency
            extent = related.find('.//mods:extent[@unit="pages"]', MODS_NS)
            if extent is not None:
                start = extent.find('mods:start', MODS_NS)
                end = extent.find('mods:end', MODS_NS)
                if start is not None and start.text:
                    granule_info['first_page'] = int(start.text) if start.text.isdigit() else None
                if end is not None and end.text:
                    granule_info['last_page'] = int(end.text) if end.text.isdigit() else None
            granule_info['package_id'] = package_id
            granules.append(granule_info)
        granules.sort(key=lambda x: (
            (x.get('agency') or 'zzz_unknown').lower(),
            x.get('first_page') or 999999))
        return granules
    
    def _download_single_granule(self, granule: Dict, package_id: str, 
                                  publish_date: datetime) -> Optional[str]:
        """Download a single granule PDF. Thread-pool safe."""
        granule_id = granule.get('granule_id')
        if not granule_id:
            return None
        download_url = CONTENT_URL_TEMPLATE.format(
            package_id=package_id, granule_id=granule_id)
        filepath = self._get_filepath_for_document(
            publish_date, granule.get('agency', 'unknown'), granule_id,
            doc_type=granule.get('section'), is_full_issue=False)
        success, checksum, file_size = self._download_pdf(download_url, filepath)
        if success:
            doc_info = {
                'document_id': granule_id, 'package_id': package_id,
                'granule_id': granule_id, 'title': granule.get('title'),
                'agency': granule.get('agency'), 'section': granule.get('section'),
                'publish_date': publish_date.date().isoformat(),
                'first_page': granule.get('first_page'),
                'last_page': granule.get('last_page'),
                'filepath': str(filepath.relative_to(self.output_dir)),
                'file_size': file_size, 'checksum': checksum,
                'download_url': download_url, 'is_full_issue': False}
            self.manifest.add_document(doc_info)
            self.manifest.remove_failed_download(granule_id)
            self.logger.info(f"  Downloaded: {granule_id}")
            return granule_id
        else:
            self.manifest.add_failed_download(
                granule_id, package_id, download_url, "Download failed")
            return None
    
    def process_package(self, package_id: str, publish_date: datetime) -> int:
        """
        Process a Federal Register package, dynamically detecting format.
        Returns number of NEW documents downloaded.
        """
        self.logger.info(f"Processing {package_id} ({publish_date.date()})")
        mods = self.get_mods_metadata(package_id)
        if mods is None:
            self.logger.warning(f"Could not get MODS metadata for {package_id}, trying full issue")
            downloaded = self._download_full_issue(package_id, publish_date)
            if downloaded >= 0:
                self.manifest.mark_package_complete(package_id, max(downloaded, 1))
            return max(downloaded, 0)
        
        granules = self.parse_granules_from_mods(mods, package_id)
        if not granules:
            self.logger.info(f"  No individual granules found, downloading full issue")
            downloaded = self._download_full_issue(package_id, publish_date)
            if downloaded >= 0:
                self.manifest.mark_package_complete(package_id, max(downloaded, 1))
            return max(downloaded, 0)
        
        total_granule_count = len(granules)
        to_download = [g for g in granules 
                       if g.get('granule_id') and not self.manifest.document_exists(g['granule_id'])]
        
        if not to_download:
            self.manifest.mark_package_complete(package_id, total_granule_count)
            return 0
        
        self.logger.info(f"  Downloading {len(to_download)}/{total_granule_count} granules in parallel (workers={MAX_DOWNLOAD_WORKERS})")
        futures = {
            self.download_executor.submit(
                self._download_single_granule, granule, package_id, publish_date
            ): granule.get('granule_id')
            for granule in to_download}
        
        downloaded = 0
        failed = 0
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    downloaded += 1
                else:
                    failed += 1
            except Exception as e:
                granule_id = futures[future]
                self.logger.error(f"  Error downloading {granule_id}: {e}")
                failed += 1
        
        if failed == 0:
            self.manifest.mark_package_complete(package_id, total_granule_count)
        else:
            self.logger.warning(
                f"  Package {package_id} incomplete: {failed} granule(s) failed. "
                f"Will be re-checked on next run.")
        return downloaded
    
    def _download_full_issue(self, package_id: str, publish_date: datetime) -> int:
        """Download a full-issue PDF (historic packages). Returns 0 or 1."""
        document_id = f"{package_id}_full_issue"
        if self.manifest.document_exists(document_id):
            return 0
        self.logger.info(f"Processing historic issue: {package_id} ({publish_date.date()})")
        download_url = FULL_ISSUE_URL_TEMPLATE.format(package_id=package_id)
        filepath = self._get_filepath_for_document(
            publish_date, None, document_id, is_full_issue=True)
        success, checksum, file_size = self._download_pdf(download_url, filepath)
        if success:
            doc_info = {
                'document_id': document_id, 'package_id': package_id,
                'granule_id': None,
                'title': f"Federal Register - {publish_date.strftime('%B %d, %Y')}",
                'agency': 'Full Issue', 'section': None,
                'publish_date': publish_date.date().isoformat(),
                'first_page': None, 'last_page': None,
                'filepath': str(filepath.relative_to(self.output_dir)),
                'file_size': file_size, 'checksum': checksum,
                'download_url': download_url, 'is_full_issue': True}
            self.manifest.add_document(doc_info)
            self.manifest.remove_failed_download(document_id)
            self.logger.info(f"  Downloaded: {document_id}")
            return 1
        else:
            self.manifest.add_failed_download(
                document_id, package_id, download_url, "Download failed")
            return 0
    
    def run_scrape(self, full_rescan: bool = False):
        """
        Main scraping method using sitemaps.
        
        Two-tier resumption strategy:
        
        TIER 1 — Sitemap level (lastmod-based skipping):
            If a previous scan date exists and this isn't --full-rescan, sitemaps
            whose lastmod hasn't changed since the last scan are skipped entirely.
        
        TIER 2 — Package level (short-circuiting within a sitemap):
            Only activates for sitemaps that were PREVIOUSLY FULLY WALKED — meaning
            every package in the sitemap was visited on a prior run that completed
            without interruption for that year.
            
            Within such sitemaps, once we encounter CONSECUTIVE_COMPLETE_THRESHOLD
            (2) consecutive completed packages, we break out and skip the rest.
            The first completed package is re-verified (one MODS fetch); the second
            triggers the break.
            
            For sitemaps that were NEVER fully walked (partially downloaded due to
            interruption, e.g. only December of a year), EVERY package is visited.
            This prevents the Swiss-cheese bug where completed packages at the top
            of a year would cause the scraper to skip all the gaps below them.
        
        Args:
            full_rescan: If True (--full-rescan flag), disables ALL optimizations.
        """
        skip_unchanged_sitemaps = False
        enable_short_circuit = not full_rescan
        
        if full_rescan:
            self.logger.info(
                "Starting FULL RESCAN - all years, no short-circuiting (newest first)")
        else:
            last_scan = self.manifest.get_last_scan_date()
            if last_scan:
                self.logger.info(f"Starting incremental scan (last scan: {last_scan.date()})")
                skip_unchanged_sitemaps = True
            else:
                self.logger.info(
                    "No previous scan found. Visiting all sitemaps (newest first). "
                    "Short-circuiting enabled only for previously fully-walked sitemaps.")
        
        sitemaps = self.get_sitemap_index()
        if not sitemaps:
            self.logger.error("Failed to fetch sitemap index")
            return
        
        total_downloaded = 0
        
        for sitemap_info in sitemaps:
            sitemap_url = sitemap_info['url']
            sitemap_lastmod = sitemap_info.get('lastmod', '')
            year = sitemap_info.get('year')
            
            # TIER 1: Skip unchanged sitemaps
            if skip_unchanged_sitemaps:
                cached_lastmod = self.manifest.get_sitemap_last_modified(sitemap_url)
                if cached_lastmod and cached_lastmod == sitemap_lastmod:
                    self.logger.info(f"Skipping unchanged sitemap for year {year}")
                    continue
            
            self.logger.info(f"Processing sitemap for year {year}: {sitemap_url}")
            packages = self.get_packages_from_sitemap(sitemap_url)
            self.logger.info(f"  Found {len(packages)} packages for year {year}")
            
            # TIER 2: Determine if short-circuiting is allowed for THIS sitemap.
            # Gate: must be enabled globally AND this specific sitemap must have
            # been fully walked on a prior run.
            sitemap_short_circuit = (
                enable_short_circuit 
                and self.manifest.is_sitemap_fully_walked(sitemap_url)
            )
            
            if sitemap_short_circuit:
                self.logger.info(
                    f"  Year {year}: previously fully walked — "
                    f"short-circuiting enabled")
            elif enable_short_circuit:
                self.logger.info(
                    f"  Year {year}: NOT previously fully walked — "
                    f"processing ALL packages (no short-circuiting)")
            
            consecutive_complete = 0
            did_short_circuit = False
            
            for idx, package_info in enumerate(packages):
                package_id = package_info['package_id']
                publish_date = package_info.get('date')
                
                if not publish_date:
                    continue
                
                # Per-package short-circuit (only for fully-walked sitemaps)
                if sitemap_short_circuit and self.manifest.is_package_complete(package_id):
                    consecutive_complete += 1
                    if consecutive_complete >= self.CONSECUTIVE_COMPLETE_THRESHOLD:
                        remaining = len(packages) - (idx + 1)
                        self.logger.info(
                            f"  Short-circuit: {consecutive_complete} consecutive "
                            f"completed packages at {package_id}. "
                            f"Skipping {remaining} older packages in year {year}.")
                        did_short_circuit = True
                        break
                    # First completed package: fall through to process_package()
                    # to re-verify (catches partial completions from interruptions)
                    self.logger.info(
                        f"  Re-verifying boundary package {package_id} "
                        f"({consecutive_complete}/{self.CONSECUTIVE_COMPLETE_THRESHOLD} "
                        f"toward short-circuit)")
                else:
                    consecutive_complete = 0
                
                try:
                    downloaded = self.process_package(package_id, publish_date)
                    total_downloaded += downloaded
                except Exception as e:
                    self.logger.error(f"Error processing {package_id}: {e}")
                    continue
            
            # Determine fully_walked status for this sitemap:
            # - If we short-circuited: preserve existing fully_walked=True
            #   (it was already fully walked before, short-circuit is valid)
            # - If we completed the loop without break: we visited every package,
            #   so this sitemap is now fully walked
            # - full_rescan always marks as fully walked
            if did_short_circuit:
                sitemap_fully_walked = True  # was already fully walked
            else:
                sitemap_fully_walked = True  # we visited every package
            
            self.manifest.set_sitemap_processed(
                sitemap_url, sitemap_lastmod or '', fully_walked=sitemap_fully_walked)
            self.logger.info(f"  Year {year}: marked as fully walked = {sitemap_fully_walked}")
        
        self.manifest.set_last_scan_date(datetime.now())
        self.logger.info(f"Scrape complete. Downloaded {total_downloaded} new documents.")
    
    def _retry_single_download(self, doc: Dict) -> Optional[str]:
        """Retry a single failed download. Thread-pool safe."""
        document_id = doc['document_id']
        package_id = doc['package_id']
        download_url = doc['download_url']
        publish_date = self._parse_date_from_package_id(package_id)
        if not publish_date:
            return None
        is_full_issue = '_full_issue' in document_id
        filepath = self._get_filepath_for_document(
            publish_date, 'unknown' if not is_full_issue else None,
            document_id, doc_type='unknown' if not is_full_issue else None,
            is_full_issue=is_full_issue)
        success, checksum, file_size = self._download_pdf(download_url, filepath)
        if success:
            doc_info = {
                'document_id': document_id, 'package_id': package_id,
                'granule_id': None if is_full_issue else document_id,
                'title': None, 'agency': None, 'section': None,
                'publish_date': publish_date.date().isoformat(),
                'first_page': None, 'last_page': None,
                'filepath': str(filepath.relative_to(self.output_dir)),
                'file_size': file_size, 'checksum': checksum,
                'download_url': download_url, 'is_full_issue': is_full_issue}
            self.manifest.add_document(doc_info)
            self.manifest.remove_failed_download(document_id)
            self.logger.info(f"  Retry successful: {document_id}")
            return document_id
        return None
    
    def retry_failed_downloads(self):
        """Retry previously failed downloads in parallel."""
        failed = self.manifest.get_failed_downloads(max_retries=5)
        if not failed:
            self.logger.info("No failed downloads to retry")
            return
        self.logger.info(f"Retrying {len(failed)} failed downloads (workers={MAX_DOWNLOAD_WORKERS})...")
        futures = {
            self.download_executor.submit(self._retry_single_download, doc): doc['document_id']
            for doc in failed}
        retried = 0
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    retried += 1
            except Exception as e:
                doc_id = futures[future]
                self.logger.error(f"  Error retrying {doc_id}: {e}")
        self.logger.info(f"Successfully retried {retried} downloads")
    
    def verify_manifest(self):
        """Verify that all files in the manifest exist on disk."""
        self.logger.info("Verifying manifest against downloaded files...")
        cursor = self.manifest.conn.cursor()
        cursor.execute('SELECT document_id, filepath, file_size, checksum FROM documents')
        missing = []
        size_mismatch = []
        for row in cursor.fetchall():
            filepath = self.output_dir / row['filepath']
            if not filepath.exists():
                missing.append(row['document_id'])
            elif row['file_size'] and filepath.stat().st_size != row['file_size']:
                size_mismatch.append(row['document_id'])
        if missing:
            self.logger.warning(f"Missing files: {len(missing)}")
            for doc_id in missing[:10]:
                self.logger.warning(f"  - {doc_id}")
            if len(missing) > 10:
                self.logger.warning(f"  ... and {len(missing) - 10} more")
        if size_mismatch:
            self.logger.warning(f"Size mismatch: {len(size_mismatch)}")
        if not missing and not size_mismatch:
            self.logger.info("All files verified successfully!")
        return missing, size_mismatch


def main():
    """Main entry point."""
    global MAX_DOWNLOAD_WORKERS
    
    parser = argparse.ArgumentParser(
        description="Download Federal Register PDFs from govinfo.gov (no API key required)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Document Types (for --types filter):
  RULE      Final rules
  PRORULE   Proposed rules  
  NOTICE    Notices
  PRESDOCU  Presidential documents
  CORRECT   Corrections
  UNKNOWN   Uncategorized

Examples:
  %(prog)s                              # Download all document types
  %(prog)s --types RULE,PRORULE         # Only final and proposed rules
  %(prog)s --types RULE --full-rescan   # Full rescan, only rules
  %(prog)s --include-frontmatter        # Include table of contents
        """)
    parser.add_argument('--full-rescan', action='store_true',
        help='Perform a full rescan of all years instead of incremental')
    parser.add_argument('--verify', action='store_true',
        help='Verify manifest against downloaded files')
    parser.add_argument('--stats', action='store_true',
        help='Show download statistics')
    parser.add_argument('--retry-failed', action='store_true',
        help='Retry previously failed downloads')
    parser.add_argument('--include-frontmatter', action='store_true',
        help='Include FrontMatter (table of contents) and ReaderAids sections')
    parser.add_argument('--types', type=str, default=None,
        help='Comma-separated list of document types to download (e.g., RULE,PRORULE,NOTICE)')
    parser.add_argument('--workers', type=int, default=None,
        help=f'Number of parallel PDF download threads (default: {MAX_DOWNLOAD_WORKERS}). Range: 1-10')
    
    args = parser.parse_args()
    
    if args.workers is not None:
        MAX_DOWNLOAD_WORKERS = max(1, min(10, args.workers))
    
    if "PUT/YOUR" in str(OUTPUT_DIR) or "PUT/YOUR" in str(MANIFEST_DB) or "PUT/YOUR" in str(LOG_FILE):
        print("ERROR: You must edit the configuration section at the top of this script.")
        print("       Set OUTPUT_DIR, MANIFEST_DB, and LOG_FILE to your desired paths.")
        sys.exit(1)
    
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Federal Register Scraper v5 (Smart Resumption)")
    logger.info("=" * 60)
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Manifest database: {MANIFEST_DB}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info(f"Download workers: {MAX_DOWNLOAD_WORKERS}")
    
    types_filter = None
    if args.types:
        types_filter = [t.strip() for t in args.types.split(',')]
        logger.info(f"Document type filter: {types_filter}")
    if args.include_frontmatter:
        logger.info("Including FrontMatter and ReaderAids")
    
    manifest = ManifestDB(MANIFEST_DB)
    
    try:
        if args.stats:
            stats = manifest.get_statistics()
            print("\n=== Federal Register Download Statistics ===")
            print(f"Total documents: {stats['total_documents']:,}")
            print(f"  - Individual granules: {stats['individual_granules']:,}")
            print(f"  - Full issues (historic): {stats['full_issues']:,}")
            print(f"Total size: {stats['total_size_gb']:.2f} GB")
            print(f"Date range: {stats['earliest_date']} to {stats['latest_date']}")
            print(f"Completed packages: {stats['completed_packages']:,}")
            print(f"Fully walked sitemaps: {stats['fully_walked_sitemaps']}/{stats['total_sitemaps_tracked']}")
            print(f"Failed downloads pending: {stats['failed_downloads']}")
            if stats['last_scan_date']:
                print(f"Last scan: {stats['last_scan_date'].strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print("Last scan: Never")
            return
        
        scraper = FederalRegisterScraper(
            OUTPUT_DIR, manifest,
            include_frontmatter=args.include_frontmatter,
            types_filter=types_filter)
        
        if args.verify:
            scraper.verify_manifest()
            return
        if args.retry_failed:
            scraper.retry_failed_downloads()
            return
        
        scraper.run_scrape(full_rescan=args.full_rescan)
        scraper.retry_failed_downloads()
        
        stats = manifest.get_statistics()
        logger.info(f"Total documents in manifest: {stats['total_documents']:,}")
        logger.info(f"Completed packages: {stats['completed_packages']:,}")
        logger.info(f"Fully walked sitemaps: {stats['fully_walked_sitemaps']}/{stats['total_sitemaps_tracked']}")
        logger.info(f"Total size: {stats['total_size_gb']:.2f} GB")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user - shutting down threads...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        if 'scraper' in locals():
            scraper.download_executor.shutdown(wait=False, cancel_futures=True)
        manifest.close()
        logger.info("Scraper finished")


if __name__ == "__main__":
    main()