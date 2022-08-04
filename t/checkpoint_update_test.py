#!/usr/bin/env python3
# coding: utf-8

from .checkpoint_update_base_test import CheckpointUpdateBaseTest

class CheckpointUpdateTest(CheckpointUpdateBaseTest):
	def test_concurrent_update_eviction_single_checkpoint(self):
		self.concurrent_update_eviction_base(False, False, False, 0)

	def test_concurrent_update_eviction_first_checkpoint(self):
		self.concurrent_update_eviction_base(False, False, False, 3)

	def test_concurrent_update_eviction_middle_checkpoint(self):
		self.concurrent_update_eviction_base(False, True, False, 1)

	def test_concurrent_update_eviction_many_checkpoints(self):
		self.concurrent_update_eviction_base(False, True, True, 5, False)

	def test_concurrent_update_eviction_many_update_checkpoints(self):
		self.concurrent_update_eviction_base(False, True, True, 5)

	def test_checkpoint_rll(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id int NOT NULL,\n"
					   "	value text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n"
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT id, repeat('x', 250) || id FROM generate_series(1, 1000, 1) id);\n")
		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 20 FOR KEY SHARE;")
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 10 FOR SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 11 and 20 FOR NO KEY UPDATE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 21 and 30 FOR UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()
		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 1000)
		node.stop()

	def test_checkpoint_rll2(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_checkpoint
				(SELECT id FROM generate_series(1, 3) id);
		""")
		node.safe_psql("""
			CREATE TABLE o_test(a int PRIMARY KEY) USING orioledb;
		""")
		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()

	def test_checkpoint_rll3(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				val_1 int,
				val_2 int,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_checkpoint
				(SELECT id, id, id FROM generate_series(1, 3) id);
		""")

		node.safe_psql("""
		  CREATE TYPE o_enum AS ENUM ('a', 'b');
		  CREATE TABLE o_test_typecaches (key o_enum, PRIMARY KEY(key)) USING orioledb;
		""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()

	def test_checkpoint_rll4(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				val_1 int,
				val_2 int,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_checkpoint
				(SELECT id, id, id FROM generate_series(1, 3) id);
		""")

		node.safe_psql("""
     		UPDATE o_checkpoint SET val_2 = 10 WHERE val_1 = 2;
    	""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()

	def test_checkpoint_rll5(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				val_1 int,
				val_2 int,
				val_3 int,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_checkpoint
				(SELECT id, id, id, id FROM generate_series(1, 3) id);
		""")

		node.safe_psql("""
     		DELETE FROM o_checkpoint WHERE val_3 = 1;
    	""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 2)
		node.stop()

	def test_checkpoint_rll6(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				val_1 int,
				val_2 int,
				val_3 int,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_checkpoint
				(SELECT id, id, id, id FROM generate_series(1, 3) id);
		""")

		node.safe_psql("""
     		TRUNCATE o_checkpoint;
			INSERT INTO o_checkpoint
				(SELECT id, id, id, id FROM generate_series(1, 3) id);
    	""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()

	def test_checkpoint_rll7(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				val_1 int,
				val_2 int,
				val_3 int,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_checkpoint
				(SELECT id, id, id, id FROM generate_series(1, 3) id);
		""")

		node.safe_psql("""
     		ALTER TABLE o_checkpoint DROP COLUMN val_3;
    	""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()

	def test_checkpoint_rll8(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				val_1 int,
				val_2 int,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_checkpoint
				(SELECT id, id, id FROM generate_series(1, 3) id);
		""")

		node.safe_psql("""
     		ALTER TABLE o_checkpoint ADD COLUMN val_3 int;
    	""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()

	def test_checkpoint_rll9(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				val_1 int,
				val_2 int,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_checkpoint
				(SELECT id, id, id FROM generate_series(1, 3) id);
		""")

		node.safe_psql("""
     		ALTER TABLE o_checkpoint RENAME COLUMN val_2 TO val_3;
    	""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()

	def test_checkpoint_rll10(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL,
				PRIMARY KEY (id)
			)USING orioledb;
		""")

		node.safe_psql("""
     		INSERT INTO o_checkpoint
				(SELECT id FROM generate_series(1, 3) id);
    	""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()

	def test_checkpoint_rll11(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id int NOT NULL
			)USING orioledb;

			INSERT INTO o_checkpoint
				(SELECT id FROM generate_series(1, 3) id);
		""")

		node.safe_psql("""
     		ALTER TABLE o_checkpoint ADD PRIMARY KEY(id);
    	""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 1 and 2 FOR KEY SHARE;")
		con2.execute("SELECT * FROM o_checkpoint WHERE id BETWEEN 2 and 3 FOR NO KEY UPDATE;")

		node.safe_psql("CHECKPOINT;")

		con1.commit()
		con2.commit()

		con1.close()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()

		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 3)
		node.stop()
