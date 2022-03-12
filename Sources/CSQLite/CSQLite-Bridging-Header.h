#include <sqlite3.h>

typedef void(*logCallback)(void *p, int iErrCode, const char*zFormat);

static inline void registerLogCallback(logCallback callback) {
  sqlite3_config(SQLITE_CONFIG_LOG, callback, 0);
}

#if SQLITE_VERSION_NUMBER >= 3029000

static inline void disableDoubleQuotedStringLiterals(sqlite3 *db) {
  sqlite3_db_config(db, SQLITE_DBCONFIG_DQS_DDL, 0, (void *)0);
  sqlite3_db_config(db, SQLITE_DBCONFIG_DQS_DML, 0, (void *)0);
}
static inline void enableDoubleQuotedStringLiterals(sqlite3 *db) {
  sqlite3_db_config(db, SQLITE_DBCONFIG_DQS_DDL, 1, (void *)0);
  sqlite3_db_config(db, SQLITE_DBCONFIG_DQS_DML, 1, (void *)0);
}

#else

static inline void disableDoubleQuotedStringLiterals(sqlite3 *db) { }
static inline void enableDoubleQuotedStringLiterals(sqlite3 *db) { }

#endif
