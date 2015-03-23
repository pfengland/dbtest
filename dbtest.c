/*
dbtest.c

berkely db tests
*/

#include <db.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const int description_size = 255;
char * const name = "Planet Rock";

typedef struct database_s database;
struct database_s {
    DB_ENV *env;
    DB *dbp;
};

void db_error(const DB_ENV *dbenv, const char *prefix, const char *msg) {
    printf("db error: %s: %s\n", prefix, msg);
}

void connect(database *db) {

    u_int32_t db_flags;
    u_int32_t env_flags;
    int ret;

    if ((ret = db_env_create(&db->env, 0)) != 0) {
	printf("db_env_create failed\n");
	exit(1);
    }

    env_flags = DB_CREATE | DB_INIT_MPOOL;

    if ((ret = db->env->open(db->env, "/home/forrest/projects/db/testenv", 
			 env_flags, 0))) {
	printf("opening env failed\n");
	exit(1);
    }

    if ((ret = db_create(&db->dbp, db->env, 0)) != 0) {
	printf("db_create failed\n");
	exit(1);
    }

    db->dbp->set_errcall(db->dbp, db_error);
    db->dbp->set_errpfx(db->dbp, "dbtest");

    db_flags = DB_CREATE;

    ret = db->dbp->open(db->dbp, NULL, "records.db", NULL, DB_BTREE, db_flags, 0);
    if (ret != 0) {
	printf("db_create failed\n");
	exit(1);
    }
}

void create_record(database *db) {

    int ret;
    DBT key, data;
    char *name = "Planet Rock";
    char *description = "Planet Rock, Kraftwerk Trans Europe Express, Numbers";

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data = name;
    key.size = strlen(name) + 1;

    data.data = description;
    data.size = strlen(description) + 1;

    if ((ret = db->dbp->put(db->dbp, NULL, &key, &data, DB_NOOVERWRITE)) != 0) {
	if (ret == DB_KEYEXIST) {
	    db->dbp->err(db->dbp, ret, 
			 "put failed because key '%s' already exists", name);
	}
    }
}

void read_record(database *db) {

    int ret;
    DBT key, data;
    char description[description_size + 1];

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data = name;
    key.size = strlen(name) + 1;

    data.data = description;
    data.ulen = description_size + 1;
    data.flags = DB_DBT_USERMEM;

    if ((ret = db->dbp->get(db->dbp, NULL, &key, &data, 0)) != 0) {
	printf("db get failed\n");
	exit(1);
    }
    
    printf("got description for key '%s' : '%s'\n", name, description);
}

void delete_record(database *db) {

    int ret;
    DBT key;

    memset(&key, 0, sizeof(DBT));

    key.data = name;
    key.size = strlen(name) + 1;

    if ((ret = db->dbp->del(db->dbp, NULL, &key, 0)) != 0) {
	printf("db del failed\n");
	exit(1);
    }
}

void cleanup(database *db) {
    db->dbp->close(db->dbp, 0);
    db->env->close(db->env, 0);
}

int main(int argc, char *argv[]) {

    database db;

    connect(&db);

    delete_record(&db);
    create_record(&db);
    read_record(&db);

    cleanup(&db);

    return 0;
}
