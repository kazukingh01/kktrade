from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE

__all__ = [
    "HOST_FR",
    "PORT_FR",
    "USER_FR",
    "PASS_FR",
    "DBNAME_FR",
    "DBTYPE_FR",
    "HOST_TO",
    "PORT_TO",
    "USER_TO",
    "PASS_TO",
    "DBNAME_TO",
    "DBTYPE_TO",
]

HOST_FR   = HOST  
PORT_FR   = PORT  
USER_FR   = USER  
PASS_FR   = PASS  
DBNAME_FR = DBNAME
DBTYPE_FR = DBTYPE

HOST_TO   = "127.0.0.1"
PORT_TO   = 22017
USER_TO   = "admin"
PASS_TO   = "admin"
DBNAME_TO = "trade"
DBTYPE_TO = "mongo"
