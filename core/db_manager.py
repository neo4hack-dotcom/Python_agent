"""
DB Manager - ClickHouse (pure HTTP, no driver) + Oracle (python-oracledb thin mode)
Zero Oracle Client installation required.
"""
import json
import urllib.request
import urllib.parse
import urllib.error
import time
from typing import List, Dict, Any, Optional, Tuple


class DBError(Exception):
    pass


class ClickHouseClient:
    """
    Client ClickHouse via HTTP natif.
    Aucune dépendance externe - utilise uniquement urllib.
    ClickHouse expose son API HTTP sur le port 8123 par défaut.
    """

    def __init__(self, config: dict):
        self.host     = config.get("host", "localhost")
        self.port     = int(config.get("port", 8123))
        self.database = config.get("database", "default")
        self.user     = config.get("user", "default")
        self.password = config.get("password", "")
        self.secure   = config.get("secure", False)
        self.timeout  = int(config.get("timeout", 30))
        scheme = "https" if self.secure else "http"
        self.base_url = f"{scheme}://{self.host}:{self.port}/"

    def _build_url(self, extra_params: dict = None) -> str:
        params = {
            "database": self.database,
            "user":     self.user,
        }
        if self.password:
            params["password"] = self.password
        if extra_params:
            params.update(extra_params)
        return self.base_url + "?" + urllib.parse.urlencode(params)

    def execute(self, query: str, max_rows: int = 10000) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return list of dicts."""
        # Force JSON output from ClickHouse
        full_query = query.rstrip(";").rstrip() + " FORMAT JSONEachRow"
        url = self._build_url({"max_result_rows": str(max_rows)})
        data = full_query.encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "text/plain; charset=utf-8")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8", errors="replace")
            raise DBError(f"ClickHouse HTTP {e.code}: {err}") from e
        except urllib.error.URLError as e:
            raise DBError(f"Cannot reach ClickHouse at {self.base_url}: {e.reason}") from e

        rows = []
        for line in raw.strip().splitlines():
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return rows

    def execute_ddl(self, query: str) -> str:
        """Execute DDL/write queries (CREATE, INSERT, ALTER…). Returns ClickHouse response."""
        url = self._build_url()
        data = query.encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "text/plain; charset=utf-8")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8", errors="replace")
            raise DBError(f"ClickHouse HTTP {e.code}: {err}") from e

    def get_schema(self) -> Dict[str, List[Dict]]:
        """Returns {table_name: [{name, type, comment}, …]} for the current database."""
        rows = self.execute(
            "SELECT table, name, type, comment "
            "FROM system.columns "
            f"WHERE database = '{self.database}' "
            "ORDER BY table, position"
        )
        schema: Dict[str, List] = {}
        for r in rows:
            tbl = r.get("table", "?")
            schema.setdefault(tbl, []).append({
                "name":    r.get("name"),
                "type":    r.get("type"),
                "comment": r.get("comment", ""),
            })
        return schema

    def get_tables(self) -> List[str]:
        rows = self.execute(
            f"SELECT name FROM system.tables WHERE database = '{self.database}' ORDER BY name"
        )
        return [r["name"] for r in rows]

    def ping(self) -> bool:
        try:
            url = self.base_url + "ping"
            with urllib.request.urlopen(url, timeout=5) as resp:
                return resp.read().decode() == "Ok.\n"
        except Exception:
            return False

    def get_sample(self, table: str, n: int = 5) -> List[Dict]:
        return self.execute(f"SELECT * FROM {table} LIMIT {n}")

    def get_stats(self, table: str, column: str) -> Dict[str, Any]:
        """Compute basic statistics for a numeric column."""
        rows = self.execute(
            f"SELECT "
            f"  count() AS cnt, "
            f"  countIf({column} IS NULL) AS nulls, "
            f"  min({column}) AS min_val, "
            f"  max({column}) AS max_val, "
            f"  avg({column}) AS avg_val, "
            f"  stddevPop({column}) AS stddev "
            f"FROM {table}"
        )
        return rows[0] if rows else {}


class OracleClient:
    """
    Client Oracle via python-oracledb en mode THIN (aucun Oracle Client requis).
    Installé via: pip install python-oracledb
    """

    def __init__(self, config: dict):
        self.host         = config.get("host", "localhost")
        self.port         = int(config.get("port", 1521))
        self.service_name = config.get("service_name", "ORCL")
        self.user         = config.get("user", "")
        self.password     = config.get("password", "")
        self.thick_mode   = config.get("thick_mode", False)
        self.timeout      = int(config.get("timeout", 30))
        self._conn = None

    def _get_connection(self):
        if self._conn is None:
            try:
                import oracledb  # type: ignore
                if self.thick_mode:
                    oracledb.init_oracle_client()
                dsn = oracledb.makedsn(self.host, self.port, service_name=self.service_name)
                self._conn = oracledb.connect(
                    user=self.user,
                    password=self.password,
                    dsn=dsn,
                    tcp_connect_timeout=self.timeout,
                )
            except ImportError:
                raise DBError(
                    "python-oracledb not installed. Run: pip install python-oracledb"
                )
            except Exception as e:
                raise DBError(f"Oracle connection failed: {e}") from e
        return self._conn

    def execute(self, query: str, max_rows: int = 10000) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            rows = []
            for i, row in enumerate(cursor):
                if i >= max_rows:
                    break
                rows.append(dict(zip(columns, row)))
            return rows
        except Exception as e:
            raise DBError(f"Oracle query failed: {e}\nQuery: {query[:200]}") from e
        finally:
            cursor.close()

    def execute_ddl(self, query: str) -> str:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
            return "OK"
        except Exception as e:
            conn.rollback()
            raise DBError(f"Oracle DDL failed: {e}") from e
        finally:
            cursor.close()

    def get_tables(self) -> List[str]:
        rows = self.execute(
            "SELECT table_name FROM user_tables ORDER BY table_name"
        )
        return [r["table_name"] for r in rows]

    def get_schema(self) -> Dict[str, List[Dict]]:
        rows = self.execute(
            "SELECT table_name, column_name, data_type, data_length, nullable "
            "FROM user_tab_columns ORDER BY table_name, column_id"
        )
        schema: Dict[str, List] = {}
        for r in rows:
            tbl = r.get("table_name", "?")
            schema.setdefault(tbl, []).append({
                "name":     r.get("column_name"),
                "type":     r.get("data_type"),
                "nullable": r.get("nullable"),
            })
        return schema

    def ping(self) -> bool:
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.close()
            return True
        except Exception:
            return False

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


class DBManager:
    """
    Gestionnaire centralisé pour toutes les connexions BDD.
    Cache le schéma pour éviter les requêtes répétées.
    """

    def __init__(self, config: dict):
        self.config = config
        self._clickhouse: Optional[ClickHouseClient] = None
        self._oracle:     Optional[OracleClient]     = None
        self._schema_cache: Dict[str, Any] = {}
        self._query_cache:  Dict[str, Tuple[Any, float]] = {}   # {hash: (result, ts)}
        self._cache_ttl = 300  # seconds

        ch_cfg = config.get("clickhouse", {})
        if ch_cfg.get("enabled", False):
            self._clickhouse = ClickHouseClient(ch_cfg)

        or_cfg = config.get("oracle", {})
        if or_cfg.get("enabled", False):
            self._oracle = OracleClient(or_cfg)

    # -------------------------------------------------------- #
    #  Query execution (with security guard + result caching)   #
    # -------------------------------------------------------- #

    def query(
        self,
        sql: str,
        db: str = "clickhouse",
        max_rows: int = 10000,
        allow_write: bool = False,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL on the given database.
        db = 'clickhouse' | 'oracle'
        """
        sql_clean = sql.strip()
        self._security_check(sql_clean, allow_write)

        cache_key = f"{db}:{sql_clean}"
        if use_cache:
            cached = self._query_cache.get(cache_key)
            if cached and (time.time() - cached[1]) < self._cache_ttl:
                return cached[0]

        client = self._get_client(db)
        result = client.execute(sql_clean, max_rows=max_rows)

        if use_cache:
            self._query_cache[cache_key] = (result, time.time())

        return result

    def execute_write(self, sql: str, db: str = "clickhouse") -> str:
        """Execute a write (DDL/DML) query."""
        client = self._get_client(db)
        return client.execute_ddl(sql)

    # -------------------------------------------------------- #
    #  Schema helpers                                           #
    # -------------------------------------------------------- #

    def get_schema(self, db: str = "clickhouse") -> Dict[str, List[Dict]]:
        if db not in self._schema_cache:
            client = self._get_client(db)
            self._schema_cache[db] = client.get_schema()
        return self._schema_cache[db]

    def get_tables(self, db: str = "clickhouse") -> List[str]:
        return list(self.get_schema(db).keys())

    def describe_table(self, table: str, db: str = "clickhouse") -> List[Dict]:
        schema = self.get_schema(db)
        return schema.get(table, [])

    def get_sample(self, table: str, db: str = "clickhouse", n: int = 5) -> List[Dict]:
        client = self._get_client(db)
        return client.get_sample(table, n) if hasattr(client, "get_sample") else \
               client.execute(f"SELECT * FROM {table} LIMIT {n}")

    def invalidate_schema_cache(self, db: str = None):
        if db:
            self._schema_cache.pop(db, None)
        else:
            self._schema_cache.clear()

    # -------------------------------------------------------- #
    #  Status                                                   #
    # -------------------------------------------------------- #

    def status(self) -> Dict[str, bool]:
        result = {}
        if self._clickhouse:
            result["clickhouse"] = self._clickhouse.ping()
        if self._oracle:
            result["oracle"] = self._oracle.ping()
        return result

    def available_databases(self) -> List[str]:
        dbs = []
        if self._clickhouse:
            dbs.append("clickhouse")
        if self._oracle:
            dbs.append("oracle")
        return dbs

    # -------------------------------------------------------- #
    #  Internal                                                 #
    # -------------------------------------------------------- #

    def _get_client(self, db: str):
        if db == "clickhouse":
            if not self._clickhouse:
                raise DBError("ClickHouse is not enabled in config")
            return self._clickhouse
        elif db == "oracle":
            if not self._oracle:
                raise DBError("Oracle is not enabled in config")
            return self._oracle
        else:
            raise DBError(f"Unknown database: {db}. Use 'clickhouse' or 'oracle'.")

    @staticmethod
    def _security_check(sql: str, allow_write: bool):
        upper = sql.upper().strip()
        write_keywords = ["DROP ", "TRUNCATE ", "DELETE ", "UPDATE ", "INSERT ",
                          "ALTER ", "CREATE ", "REPLACE ", "RENAME "]
        for kw in write_keywords:
            if upper.startswith(kw) or f" {kw}" in upper:
                if not allow_write:
                    raise DBError(
                        f"Write query blocked by security policy (allow_write=False): {sql[:80]}"
                    )
                break

    def close(self):
        if self._oracle:
            self._oracle.close()
