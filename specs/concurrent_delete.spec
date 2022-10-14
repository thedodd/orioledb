# TODO: Add insert on conflict/concurrent delete test
# TODO: Add insert to secondary idx/concurrent delete test
# TODO: Add concurrent reinsert/update test
# TODO: Replace all DECLARE blocks with simple orioledb_tbl_structure 'nue' call
# TODO: Rename file if there will be not only concurrent delete test
setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE o_test_1 (
		val_1 int PRIMARY KEY,
		val_2 int
	) USING orioledb;

	INSERT INTO o_test_1 (SELECT val_1, val_1 + 100
							FROM generate_series(1, 5) val_1);
}

teardown
{
	DROP TABLE o_test_1;
}

session "s1"

step "begin_1" { BEGIN; }
step "select_1" {
					SELECT * FROM o_test_1 ORDER BY val_1;
					DO LANGUAGE plpgsql
					$$
					DECLARE
						relname text = 'o_test_1';
						reloid oid = relname::regclass::oid;
						amname pg_catalog.text := (SELECT amname FROM pg_class pc
														JOIN pg_am pa ON relam = pa.oid
														WHERE pc.oid = reloid);
					BEGIN
						IF amname = 'orioledb' THEN
							RAISE NOTICE '%', (SELECT amname);
							RAISE NOTICE '%', orioledb_tbl_structure(reloid, 'bUCKSivo');
						END IF;
					END $$;
				}
step "update_1" { UPDATE o_test_1 SET val_1 = val_1 + 10 WHERE val_1 = 1; }
step "update_1_2" { UPDATE o_test_1 SET val_2 = val_2 + 10 WHERE val_1 = 1; }
step "update_1_3" { UPDATE o_test_1 SET val_1 = val_1 + 10 WHERE val_1 < 10; }
step "update_1_4" {
	UPDATE o_test_1 SET val_1 = val_1 + 10 WHERE val_1 < 4;
	UPDATE o_test_1 SET val_1 = val_1 - 10 WHERE val_1 = 4;
}
step "update_1_5" { UPDATE o_test_1 SET val_2 = val_2 + 10; }
step "insert_1" { INSERT INTO o_test_1 VALUES (100, 1), (200, 2); }
step "insert_1_2" { INSERT INTO o_test_1 VALUES (1, 1); }
step "delete_1" { DELETE FROM o_test_1 WHERE val_1 = 1; }
step "delete_1_2" { DELETE FROM o_test_1 WHERE val_1 IN (1, 2); }
step "commit_1" { COMMIT;
					DO LANGUAGE plpgsql
					$$
					DECLARE
						relname text = 'o_test_1';
						reloid oid = relname::regclass::oid;
						amname pg_catalog.text := (SELECT amname FROM pg_class pc
														JOIN pg_am pa ON relam = pa.oid
														WHERE pc.oid = reloid);
					BEGIN
						IF amname = 'orioledb' THEN
							RAISE NOTICE '%', (SELECT amname);
							RAISE NOTICE '%', orioledb_tbl_structure(reloid, 'bUCKSivo');
						END IF;
					END $$;}
step "rollback_1" { ROLLBACK; }
step "pg_locks" {
	SELECT (string_to_array(psa.application_name, '/'))[3] name,
		   rpad(pl.locktype, 13) locktype, psa.datname, pc.relname,
		   rpad(pl.page::text, 10) page, pl.tuple,
		   pl.virtualxid, pl.transactionid, pl.virtualtransaction,
		   rpad(pl.mode, 19) mode, pl.granted, pl.fastpath, pl.waitstart
			FROM pg_locks pl
		LEFT JOIN pg_stat_activity psa ON pl.pid = psa.pid
		LEFT JOIN pg_class pc ON pl.relation = pc.oid
			WHERE psa.datname = 'isolation_regression' OR
				locktype = 'virtualxid'
			ORDER BY relname, locktype, name, mode;
}

session "s2"

step "begin_2" { BEGIN; }
step "select_2" {
					SELECT * FROM o_test_1 ORDER BY val_1;
					DO LANGUAGE plpgsql
					$$
					DECLARE
						relname text = 'o_test_1';
						reloid oid = relname::regclass::oid;
						amname pg_catalog.text := (SELECT amname FROM pg_class pc
														JOIN pg_am pa ON relam = pa.oid
														WHERE pc.oid = reloid);
					BEGIN
						IF amname = 'orioledb' THEN
							RAISE NOTICE '%', (SELECT amname);
							RAISE NOTICE '%', orioledb_tbl_structure(reloid, 'bUCKSivo');
						END IF;
					END
					$$;
				}
step "savepoint_2" { SAVEPOINT s1; }
step "update_2" { UPDATE o_test_1 SET val_1 = val_1 + 20 WHERE val_1 = 1; }
step "update_2_2" { UPDATE o_test_1 SET val_2 = val_2 + 20 WHERE val_1 = 1; }
step "expl_update_2_3" { EXPLAIN (COSTS OFF) UPDATE o_test_1 SET val_2 = val_2 + 20 WHERE val_1 < 4; }
step "update_2_3" { UPDATE o_test_1 SET val_2 = val_2 + 20 WHERE val_1 < 4; }
step "update_2_4" { UPDATE o_test_1 SET val_2 = val_2 + 20; }
step "update_2_5" { UPDATE o_test_1 SET val_2 = val_2 + 20 WHERE val_1 > 10; }
step "update_2_6" { UPDATE o_test_1 SET val_2 = val_2 + 20 WHERE val_1 IN (1, 2); }
step "delete_2" {
	DELETE FROM o_test_1 WHERE val_1 = 1;
	DO LANGUAGE plpgsql
	$$
	DECLARE
		relname text = 'o_test_1';
		reloid oid = relname::regclass::oid;
		amname pg_catalog.text := (SELECT amname FROM pg_class pc
										JOIN pg_am pa ON relam = pa.oid
										WHERE pc.oid = reloid);
	BEGIN
		IF amname = 'orioledb' THEN
			RAISE NOTICE '%', (SELECT amname);
			RAISE NOTICE '%', orioledb_tbl_structure(reloid, 'bUCKSivo');
		END IF;
	END
	$$;
}
step "insert_2" { INSERT INTO o_test_1 VALUES (1, 2); }
step "rollback_to_savepoint_2" { ROLLBACK TO s1; }
step "commit_2" { COMMIT; }

session "s3"
step "begin_3" { BEGIN; }
step "insert_3" { INSERT INTO o_test_1 VALUES (2, 3); }
step "insert_3_2" { INSERT INTO o_test_1 VALUES (1, 3), (2, 3); }
step "select_3" {
					SELECT * FROM o_test_1 ORDER BY val_1;
					DO LANGUAGE plpgsql
					$$
					DECLARE
						relname text = 'o_test_1';
						reloid oid = relname::regclass::oid;
						amname pg_catalog.text := (SELECT amname FROM pg_class pc
														JOIN pg_am pa ON relam = pa.oid
														WHERE pc.oid = reloid);
					BEGIN
						-- IF amname = 'orioledb' THEN
						-- 	RAISE NOTICE '%', (SELECT amname);
						-- 	RAISE NOTICE '%', orioledb_tbl_structure(reloid, 'bUCKSivo');
						-- END IF;
					END
					$$;
				}
step "update_3" {
	UPDATE o_test_1 SET val_2 = 3 WHERE val_1 = 1;
	DO LANGUAGE plpgsql
	$$
	DECLARE
		relname text = 'o_test_1';
		reloid oid = relname::regclass::oid;
		amname pg_catalog.text := (SELECT amname FROM pg_class pc
										JOIN pg_am pa ON relam = pa.oid
										WHERE pc.oid = reloid);
	BEGIN
		IF amname = 'orioledb' THEN
			RAISE NOTICE '%', (SELECT amname);
			RAISE NOTICE '%', orioledb_tbl_structure(reloid, 'bUCKSivo');
		END IF;
	END
	$$;
}
step "commit_3" { COMMIT; }

# permutation "begin_1" "begin_2"
# 				"select_1"
# 				"delete_1" "select_1" "select_2"
# 				"delete_2" "select_1"
# 			"commit_1"
# 				"select_1" "select_2"
# 			"commit_2"
# 				"select_1" "select_2"

# permutation "begin_1" "begin_2"
# 				"select_1"
# 				"update_1_2" "select_1" "select_2"
# 				"update_2_2"
# 			"commit_1"
# 				"select_1" "select_2"
# 			"commit_2"
# 				"select_1" "select_2"

# permutation "begin_1" "begin_2"
# 				"select_1"
# 				"savepoint_2"
# 				"update_1_2" "select_1" "select_2"
# 				"update_2_2"
# 			"commit_1"
# 				"select_1" "select_2"
# 				"rollback_to_savepoint_2"
# 				"select_1" "select_2"
# 			"commit_2"
# 				"select_1" "select_2"

# permutation "begin_1" "begin_2"
# 				"select_1"
# 				"delete_1" "select_1" "select_2"
# 				"update_2" "select_1"
# 			"commit_1"
# 				"select_1" "select_2"
# 			"commit_2"
# 				"select_1" "select_2"

# permutation "begin_1" "begin_2"
# 				"select_1"
# 				"update_1" "select_1" "select_2"
# 				"update_2" "select_1"
# 			"commit_1"
# 				"select_1" "select_2"
# 			"commit_2"
# 				"select_1" "select_2"

# permutation "begin_1" "begin_2"
# 				"select_1"
# 				"delete_1" "select_1" "select_2"
# 				"update_2_2" "select_1"
# 			"commit_1"
# 				"select_1" "select_2"
# 			"commit_2"
# 				"select_1" "select_2"

permutation "begin_1" "begin_2"
				"select_1"
				"update_1_3" "select_1" "select_2"
				"expl_update_2_3"
				"update_2_3" "select_1"
			"commit_1"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"

# permutation "insert_1" "select_1"
# 			"begin_1" "begin_2"
# 				"select_1"
# 				"update_1_4" "select_1" "select_2"
# 				"update_2_5" "select_1"
# 			"commit_1"
# 				"select_1" "select_2"
# 			"commit_2"
# 				"select_1" "select_2"

# permutation "begin_1" "begin_2"
# 				"select_1"
# 				"update_1_5" "select_1" "select_2"
# 				"insert_1" "select_1" "select_2"
# 				"update_2_3" "select_1"
# 			"commit_1"
# 				"select_1" "select_2"
# 			"commit_2"
# 				"select_1" "select_2"

# permutation "begin_1" "begin_2" "begin_3"
# 				"select_1"
# 				"delete_1"
# 				"insert_1_2"
# 				"select_1" "select_2" "select_3"
# 				"delete_2"
# 				"select_1" "select_3"
# 				"update_3"
# 				"select_1"
# 			"commit_1"
# 				"insert_2"
# 				"select_1" "select_2"
# 			"commit_2"
# 			"commit_3"
# 				"select_1" "select_2" "select_3"

permutation "begin_1" "begin_2" "begin_3"
				"select_1"
				"delete_1"
				"insert_1_2"
				"select_1" "select_2" "select_3"
				"delete_2"
				"select_1" "select_3"
				"update_3"
				"select_1"
			"rollback_1"
				"insert_2"
				"select_1" "select_2"
			"commit_2"
			"commit_3"
				"select_1" "select_2" "select_3"

permutation "begin_1" "begin_3"
				"select_1"
				"delete_1_2"
				"select_1" "select_2"
				"insert_3_2"
			"begin_2"
				"select_1" "select_2"
			"commit_1"
				"select_1" "select_2"
				"update_2_3" "select_1"
			"commit_3"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"

# TODO: Remove or rewrite
# If the first updater commits,
# the second updater will ignore the row if the first updater deleted it,
# otherwise it will attempt to apply its operation
# to the updated version of the row.
# The search condition of the command (the WHERE clause) is re-evaluated
# to see if the updated version of the row still matches the search condition.
# If so, the second updater proceeds with its operation
# using the updated version of the row.
# In the case of SELECT FOR UPDATE and SELECT FOR SHARE,
# this means it is the updated version of the row
# that is locked and returned to the client.
permutation "begin_1" "begin_2"
				"select_1"
				"update_1_4" "select_1" "select_2"
				"insert_1"
				"insert_1_2"
				"insert_3"
				"select_1" "select_2"
				"update_2_4" "select_1"
			"commit_1"
				"select_1" "select_2"
			"commit_2"
				"select_1" "select_2"
