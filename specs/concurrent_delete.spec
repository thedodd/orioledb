# TODO: Add insert on conflict/concurrent delete test
# TODO: Add insert to secondary idx/concurrent delete test
# TODO: Add concurrent reinsert/update test
setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_test_1 (
		val_1 int PRIMARY KEY,
		val_2 int
	) USING orioledb;

	INSERT INTO o_test_1 (SELECT val_1, val_1 + 100
							FROM generate_series(1, 3) val_1);

	CREATE SEQUENCE IF NOT EXISTS test_num;
}

teardown
{
	DROP TABLE o_test_1;
}

session "s1"

step "begin_1" { BEGIN; }
step "select_1" {
					SELECT * FROM o_test_1 ORDER BY val_1;
					SELECT orioledb_tbl_structure('o_test_1'::regclass,
												  'nue');
				}
step "update_1" { UPDATE o_test_1 SET val_1 = val_1 + 10 WHERE val_1 = 1; }
step "update_1_2" { UPDATE o_test_1 SET val_2 = val_2 + 10 WHERE val_1 = 1; }
step "delete_1" { DELETE FROM o_test_1 WHERE val_1 = 1; }
step "commit_1" { COMMIT; }

session "s2"

step "begin_2" { BEGIN; }
step "select_2" {
					SELECT * FROM o_test_1 ORDER BY val_1;
					SELECT orioledb_tbl_structure('o_test_1'::regclass,
												  'nue');
				}
step "savepoint_2" { SAVEPOINT s1; }
step "update_2" { UPDATE o_test_1 SET val_1 = val_1 + 20 WHERE val_1 = 1; }
step "update_2_2" { UPDATE o_test_1 SET val_2 = val_2 + 20 WHERE val_1 = 1; }
step "delete_2" { DELETE FROM o_test_1 WHERE val_1 = 1; }
step "rollback_to_savepoint_2" { ROLLBACK TO s1; }
step "commit_2" { COMMIT; }

permutation "begin_1" "begin_2"
				"select_1"
				"delete_1" "select_1" "select_2"
				"delete_2" "select_1"
			"commit_1"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"

permutation "begin_1" "begin_2"
				"select_1"
				"update_1_2" "select_1" "select_2"
				"update_2_2"
			"commit_1"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"

permutation "begin_1" "begin_2"
				"select_1"
				"savepoint_2"
				"update_1_2" "select_1" "select_2"
				"update_2_2"
			"commit_1"
				"select_1" "select_2"
				"rollback_to_savepoint_2"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"

permutation "begin_1" "begin_2"
				"select_1"
				"delete_1" "select_1" "select_2"
				"update_2" "select_1"
			"commit_1"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"

permutation "begin_1" "begin_2"
				"select_1"
				"update_1" "select_1" "select_2"
				"update_2" "select_1"
			"commit_1"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"

permutation "begin_1" "begin_2"
				"select_1"
				"delete_1" "select_1" "select_2"
				"update_2_2" "select_1"
			"commit_1"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"
